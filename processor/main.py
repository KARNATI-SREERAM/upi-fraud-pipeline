# Processor entry point: consumes transactions.raw, validates/cleans each record, deduplicates,
# stores results in PostgreSQL, and forwards clean records to transactions.clean.
#
# Delivery guarantee: AT-LEAST-ONCE. For each batch the order is:
#   1. write every row in one DB transaction -> 2. DB commit -> 3. produce to clean + flush
#   -> 4. commit Kafka offsets.
# A crash anywhere before step 4 means Kafka re-sends the batch. That is safe because every DB
# write is idempotent (see store.py). Never commit offsets before the data is saved: a crash
# in between would silently lose those records.

import json
import logging
import os
import signal
import time
from collections import Counter

from confluent_kafka import Consumer, Producer

import store
import validate

RAW_TOPIC = "transactions.raw"
CLEAN_TOPIC = "transactions.clean"
BATCH_SIZE = 100

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("processor")

running = True
delivery_failures = 0


def handle_stop(signum, frame):
    global running
    running = False  # finish the current batch, then exit the loop


def on_delivery(err, msg):
    global delivery_failures
    if err is not None:
        delivery_failures += 1
        log.error("clean-topic delivery failed: %s", err)


def to_text(raw: bytes | None) -> str:
    # Quarantine stores the original message as text. Bad bytes become U+FFFD instead of crashing,
    # and NUL is removed because PostgreSQL TEXT cannot hold it.
    return (raw or b"").decode("utf-8", errors="replace").replace("\x00", "")


def process_batch(msgs, cur, counts: Counter) -> list[dict]:
    """Validate and store one batch. Returns the clean txns to forward to the clean topic."""
    to_forward = []
    for msg in msgs:
        if msg.error():
            log.warning("consumer error: %s", msg.error())
            continue
        counts["consumed"] += 1
        p, o, raw = msg.partition(), msg.offset(), msg.value()

        txn, reason = validate.validate(raw)
        if txn is None:
            if store.save_quarantine(cur, reason, to_text(raw), p, o) == "inserted":
                counts[f"quarantined:{reason}"] += 1
            else:
                counts["redeliveries"] += 1
            continue

        outcome = store.save_txn(cur, txn, p, o)
        if outcome == "inserted":
            counts["inserted"] += 1
            to_forward.append(txn)
        elif outcome == "redelivery":
            # Already saved, but we can't know whether it reached the clean topic before the
            # crash, so send it again. Downstream must dedupe by txn_id too.
            counts["redeliveries"] += 1
            to_forward.append(txn)
        else:  # a genuine duplicate: same txn_id, different Kafka message
            if store.save_quarantine(cur, "duplicate_txn_id", to_text(raw), p, o) == "inserted":
                counts["quarantined:duplicate_txn_id"] += 1
            else:
                counts["redeliveries"] += 1
    return to_forward


def log_counts(counts: Counter, start: float):
    quarantined = {k.split(":", 1)[1]: v for k, v in counts.items() if k.startswith("quarantined:")}
    log.info("consumed=%d inserted=%d quarantined=%d redeliveries=%d rate=%.1f msg/s reasons=%s",
             counts["consumed"], counts["inserted"], sum(quarantined.values()),
             counts["redeliveries"], counts["consumed"] / (time.monotonic() - start),
             dict(sorted(quarantined.items())))


def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    dsn = os.getenv("DATABASE_URL", "postgresql://pipeline:pipeline@localhost:5433/upi")

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        # All processors with this group id share the partitions between them. Kafka stores
        # the group's committed offsets, so a restarted processor resumes where it left off.
        "group.id": "processor",
        "enable.auto.commit": False,     # we commit manually, only after the DB commit
        # First run (no committed offset yet): start from the beginning of the topic.
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all",
                         "enable.idempotence": True})
    conn = store.connect(dsn)

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    consumer.subscribe([RAW_TOPIC])
    log.info("consuming %s as group 'processor'", RAW_TOPIC)

    counts = Counter()
    start = last_report = time.monotonic()
    try:
        while running:
            msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
            if msgs:
                # Steps 1-2: the whole batch is one DB transaction. If anything raises, the
                # process exits without committing offsets, Docker restarts it, and the batch is
                # re-read ("crash and replay" is simpler and safer than partial recovery code).
                with conn.cursor() as cur:
                    to_forward = process_batch(msgs, cur, counts)
                conn.commit()

                # Step 3: forward clean records, and wait until Kafka has them.
                for txn in to_forward:
                    producer.produce(CLEAN_TOPIC, key=txn["payer_vpa"],
                                     value=json.dumps(txn).encode(), on_delivery=on_delivery)
                if producer.flush(10) > 0 or delivery_failures:
                    raise RuntimeError("could not deliver to clean topic; not committing offsets")

                # Step 4: only now tell Kafka this batch is done.
                consumer.commit(asynchronous=False)

            if time.monotonic() - last_report >= 10:
                log_counts(counts, start)
                last_report = time.monotonic()
    finally:
        log_counts(counts, start)
        consumer.close()  # leave the group cleanly so partitions are reassigned at once
        conn.close()
        log.info("processor stopped")


if __name__ == "__main__":
    main()
