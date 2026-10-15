# Real-time UPI transaction pipeline with fraud detection

<!-- Shows what the project is and how to run it. It grows each phase; the full version comes in Phase 5. -->

A local, Docker-based streaming pipeline in the payments domain. A simulator produces UPI-style
transactions (including labelled fraud patterns and deliberately dirty records) into Kafka. A
processor validates, cleans and deduplicates them, then stores them in PostgreSQL, quarantining
bad records with a reason. Later phases add an ML fraud model and a FastAPI service.

**Status:** Phase 2 of 5. Producer, Kafka (KRaft), processor and PostgreSQL are working.

## Architecture (Phase 2)

```mermaid
flowchart LR
    P[producer<br/>Python simulator] -->|key = payer_vpa<br/>JSON| R[(transactions.raw<br/>3 partitions)]
    subgraph Kafka [Kafka broker, KRaft mode]
        R
        C[(transactions.clean)]
        F[(transactions.flagged)]
    end
    R --> PR[processor<br/>validate · clean · dedup]
    PR -->|valid, unique| T[(Postgres:<br/>transactions)]
    PR -->|rejected + reason| Q[(Postgres:<br/>quarantine)]
    PR -->|valid, unique| C
    I[kafka-init<br/>one-shot] -.creates topics.-> Kafka
```

The processor is **at-least-once**: per batch it commits to Postgres first, then forwards to
`transactions.clean`, then commits Kafka offsets. Writes are idempotent (`txn_id` primary key,
`UNIQUE(partition, offset)` on quarantine), so replays never create duplicate rows.

## Run

```bash
docker compose up --build        # add -d to run in the background
```

Optional: `cp .env.example .env` and edit it to change the rate, fraud/dirty rates or seed.

## See messages arriving

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic transactions.raw \
  --property print.key=true --property print.partition=true

docker compose logs -f producer    # summary every 10 s: sent / delivered / fraud / dirty / rate
docker compose logs -f processor   # consumed / inserted / quarantined by reason / redeliveries
```

## Look at the results in PostgreSQL

```bash
docker compose exec postgres psql -U pipeline -d upi
```
```sql
SELECT count(*) FROM transactions;
SELECT reason, count(*) FROM quarantine GROUP BY 1 ORDER BY 2 DESC;
SELECT * FROM transactions ORDER BY ts DESC LIMIT 5;
```
From your Mac: `postgresql://pipeline:pipeline@localhost:5433/upi`.

## Test

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/ruff check .
.venv/bin/pytest -v     # tests/test_store.py needs Postgres up (docker compose up -d); else skipped
```

## Measured results (Phase 2)

All numbers below come from **one run** of one script (MacBook, Apple Silicon, Docker Desktop with
7.7 GiB, 2026-10-15). The script starts from empty Kafka and Postgres, runs the pipeline,
kills the processor, restarts the producer, then replays the whole topic:

```bash
./scripts/verify_phase2.sh      # about 4 minutes; deletes existing pipeline data first
```

| What | Result |
|---|---|
| Producer rate (configured 20 txn/s) | 20.0 txn/s; 3159 sent = 3159 messages in `transactions.raw` |
| Accounting (no loss, no double count) | 3159 raw messages = 2996 transactions + 163 quarantined |
| Dirty-data detection | 163 dirty events injected = 163 quarantined (0 missed, 0 false rejects) |
| Duplicate txn_ids in `transactions` | 0, after a SIGKILL of the processor and a producer restart |
| Recovery after SIGKILL | processing resumed 46 s after the kill (Kafka's 45 s session timeout) |
| Full replay (offsets reset to earliest) | 3159 redeliveries, 0 new rows |
| Memory (all running) | Kafka 478.6 MiB, Postgres 29.1 MiB, processor 25.6 MiB, producer 10.2 MiB |

Counts differ on every run, because the transaction stream is random. What must hold every time
is that each `PASS` line in the script's output passes. The full output of this run is in LEARNING.md (Phase 2).

Throughput and latency benchmarks come in Phase 5.
