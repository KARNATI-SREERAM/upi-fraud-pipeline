# Benchmark: POST /score latency (p50/p95/p99). One client, sequential requests over a single
# keep-alive HTTP connection, payers taken from the database so each request has real history.
# Run from the project folder with the stack up: .venv/bin/python benchmark/api_latency.py

import argparse
import http.client
import json
import random
import statistics
import time
import uuid
from datetime import datetime, timezone

import psycopg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests", type=int, default=1000)
    ap.add_argument("--warmup", type=int, default=50)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--dsn", default="postgresql://pipeline:pipeline@localhost:5433/upi")
    args = ap.parse_args()

    with psycopg.connect(args.dsn) as conn:
        payers = [r[0] for r in conn.execute(
            "SELECT DISTINCT payer_vpa FROM transactions LIMIT 500").fetchall()]
    rng = random.Random(0)
    conn = http.client.HTTPConnection(args.host, args.port, timeout=10)

    def one_request() -> tuple[float, int]:
        body = json.dumps({
            "txn_id": str(uuid.uuid4()), "payer_vpa": rng.choice(payers),
            "payee_vpa": "food.store1@ybl", "amount": round(rng.uniform(50, 5000), 2),
            "timestamp": datetime.now(timezone.utc).isoformat(), "device_id": "dev-bench",
            "location": "Pune", "merchant_category": "food"})
        start = time.perf_counter()
        conn.request("POST", "/score", body, {"Content-Type": "application/json"})
        resp = conn.getresponse()
        resp.read()  # must read the body before the connection can be reused
        return (time.perf_counter() - start) * 1000, resp.status

    for _ in range(args.warmup):  # warm-up: connection set-up, DB pool, CPU caches
        one_request()
    results = [one_request() for _ in range(args.requests)]
    latencies = [ms for ms, _ in results]
    errors = sum(status != 200 for _, status in results)
    q = statistics.quantiles(latencies, n=100)  # q[49] = p50, q[94] = p95, q[98] = p99
    print(f"POST /score: {args.requests} requests (after {args.warmup} warm-up), "
          f"{len(payers)} distinct payers, errors={errors}")
    print(f"latency ms: p50={q[49]:.2f} p95={q[94]:.2f} p99={q[98]:.2f} "
          f"mean={statistics.mean(latencies):.2f} max={max(latencies):.2f}")


if __name__ == "__main__":
    main()
