# Benchmark: pipeline throughput (messages/sec through validate -> features -> score -> Postgres).
# 1. stop the processor  2. load N messages into transactions.raw (a backlog)
# 3. start the processor and time how long until all N are stored (every message becomes exactly
#    one row in transactions or quarantine, so "stored" is easy to count).
# Event timestamps are spaced like a 20 txn/s stream that happened HOURS_AGO hours ago, so the
# features look realistic and don't mix with the live run's history.
# Run from the project folder with the stack up: .venv/bin/python benchmark/throughput.py

import argparse
import json
import random
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg
from confluent_kafka import Producer

sys.path.insert(0, "producer")  # reuse the producer's generator (same users, same patterns)
import generator  # noqa: E402

COUNT_SQL = "SELECT (SELECT count(*) FROM transactions) + (SELECT count(*) FROM quarantine)"


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20_000)
    ap.add_argument("--hours-ago", type=float, default=6)
    ap.add_argument("--tps", type=float, default=20, help="event-time spacing, not send rate")
    ap.add_argument("--bootstrap", default="localhost:9094")
    ap.add_argument("--dsn", default="postgresql://pipeline:pipeline@localhost:5433/upi")
    args = ap.parse_args()

    compose("stop", "processor")
    with psycopg.connect(args.dsn) as conn:
        before = conn.execute(COUNT_SQL).fetchone()[0]

    # --- load the backlog as fast as the client can send it
    world = generator.make_world(2000, random.Random(42))  # same population as the live producer
    rng = random.Random()  # fresh txn_ids on every run (see producer/main.py)
    clock = datetime.now(timezone.utc) - timedelta(hours=args.hours_ago)
    step = timedelta(seconds=1 / args.tps)
    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": "all",
                         "enable.idempotence": True, "linger.ms": 20})
    sent = dirty = fraud_txns = 0
    t0 = time.perf_counter()
    while sent < args.n:
        kind, records = generator.next_batch(world, rng, clock, 0.02, 0.05)
        dirty += kind.startswith("dirty")
        # A whole event is always sent (never cut off mid-burst or mid-duplicate), so `sent` can
        # end slightly above --n; the exact count is printed and used for the accounting check.
        for rec in records:
            fraud_txns += bool(rec.get("is_fraud"))
            while True:
                try:
                    producer.produce("transactions.raw", key=rec.get("payer_vpa") or None,
                                     value=json.dumps(rec).encode())
                    break
                except BufferError:
                    producer.poll(0.5)
            producer.poll(0)
            sent += 1
        clock += step * len(records)
    producer.flush(30)
    load_secs = time.perf_counter() - t0
    print(f"backlog loaded: sent={sent} dirty_events={dirty} fraud_labelled_txns={fraud_txns} "
          f"in {load_secs:.1f} s ({sent / load_secs:.0f} msg/s from the producer client)")

    # --- time the processor draining it
    t0 = time.perf_counter()
    compose("start", "processor")
    with psycopg.connect(args.dsn, autocommit=True) as conn:
        while conn.execute(COUNT_SQL).fetchone()[0] < before + sent:
            time.sleep(0.2)
    secs = time.perf_counter() - t0
    print(f"processor: {sent} messages stored in {secs:.1f} s = {sent / secs:.0f} msg/s "
          f"(includes processor start-up)")


if __name__ == "__main__":
    main()
