# Producer entry point: generates transactions with generator.py and sends them to Kafka.
# Runs forever at a fixed rate until stopped; flushes buffered messages on shutdown.

import json
import logging
import os
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

import generator

TOPIC = "transactions.raw"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

counts = {"sent": 0, "delivered": 0, "failed": 0, "fraud_events": 0, "dirty_events": 0}
running = True


def handle_stop(signum, frame):
    # `docker compose down` sends SIGTERM. We finish the loop cleanly instead of dying mid-send.
    global running
    running = False


def on_delivery(err, msg):
    # Called by the client (inside poll/flush) once the broker confirms or rejects each message.
    if err is not None:
        counts["failed"] += 1
        log.error("delivery failed: %s", err)
    else:
        counts["delivered"] += 1


def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    rate = float(os.getenv("TXNS_PER_SEC", "20"))
    fraud_rate = float(os.getenv("FRAUD_RATE", "0.02"))
    dirty_rate = float(os.getenv("DIRTY_RATE", "0.05"))
    num_users = int(os.getenv("NUM_USERS", "500"))
    seed = int(os.getenv("SEED", "42"))

    rng = random.Random(seed)
    world = generator.make_world(num_users, rng)

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",                # broker confirms only after the write is durable
        "enable.idempotence": True,   # producer retries can't create duplicates on the broker
        "linger.ms": 5,               # wait up to 5 ms to batch messages: fewer, bigger requests
    })

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    log.info("producing to %s on %s at %.1f txn/s (seed=%d, users=%d)",
             TOPIC, bootstrap, rate, seed, num_users)

    interval = 1.0 / rate
    start = last_report = next_send = time.monotonic()

    while running:
        kind, records = generator.next_batch(world, rng, datetime.now(timezone.utc),
                                             fraud_rate, dirty_rate)
        if kind.startswith("fraud"):
            counts["fraud_events"] += 1
        elif kind.startswith("dirty"):
            counts["dirty_events"] += 1

        for rec in records:
            # Key = payer VPA, so all of one user's txns go to the same partition, in order.
            # A record with a missing payer gets key None, and Kafka picks a partition itself.
            key = rec.get("payer_vpa") or None
            while True:
                try:
                    producer.produce(TOPIC, key=key, value=json.dumps(rec).encode(),
                                     on_delivery=on_delivery)
                    break
                except BufferError:
                    # Local send queue is full (broker slow/down): wait for deliveries, then retry.
                    producer.poll(1)
            counts["sent"] += 1
            producer.poll(0)  # serve delivery callbacks without blocking

            # Pace against a schedule (not a fixed sleep) so time spent producing doesn't slow us.
            next_send += interval
            delay = next_send - time.monotonic()
            if delay > 0:
                time.sleep(delay)

        now = time.monotonic()
        if now - last_report >= 10:
            log.info("sent=%d delivered=%d failed=%d fraud_events=%d dirty_events=%d "
                     "rate=%.1f txn/s", counts["sent"], counts["delivered"], counts["failed"],
                     counts["fraud_events"], counts["dirty_events"], counts["sent"] / (now - start))
            last_report = now

    log.info("stopping: flushing buffered messages")
    remaining = producer.flush(10)  # block until everything in the buffer is delivered
    log.info("final: sent=%d delivered=%d failed=%d undelivered=%d",
             counts["sent"], counts["delivered"], counts["failed"], remaining)


if __name__ == "__main__":
    main()
