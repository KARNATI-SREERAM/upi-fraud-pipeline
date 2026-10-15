# Real-time UPI transaction pipeline with fraud detection

[![CI](https://github.com/KARNATI-SREERAM/upi-fraud-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/KARNATI-SREERAM/upi-fraud-pipeline/actions/workflows/ci.yml)

<!-- What the project is, how to run and test it, and its measured results (all from one run). -->

**Kafka → validation/dedup → ML fraud scoring → PostgreSQL → FastAPI, running locally with one command, with effectively-once storage (no lost or duplicate rows) across crashes and replays.**

## Architecture

```mermaid
flowchart LR
    T[trainer<br/>one-shot] -->|model.joblib| V[(models volume)]
    P[producer<br/>simulator] -->|key = payer_vpa| R[(transactions.raw<br/>3 partitions)]
    subgraph Kafka [Kafka, single broker, KRaft]
        R
        C[(transactions.clean)]
        F[(transactions.flagged)]
    end
    R --> PR[processor<br/>validate · dedup · features · score]
    V -.-> PR
    PR -->|valid + scored| TX[(Postgres: transactions)]
    PR -->|rejected + reason| Q[(Postgres: quarantine)]
    PR --> C
    PR -->|score ≥ 0.5| F
    TX --> API[FastAPI :8000]
    Q --> API
    V -.-> API
```

- **At-least-once, effectively-once storage.** Per batch: DB commit → produce to clean/flagged → commit Kafka offsets. Writes are idempotent (`txn_id` primary key; `UNIQUE(partition, offset)` on quarantine), so crashes and replays never lose or duplicate a row.
- **No train/serve skew.** The trainer, the processor and the API all compute features with the same function (`model/features.py`).

## Measured results

Every number here comes from **one run** of one command, saved in
[results/measured_run.txt](results/measured_run.txt) (Apple M4, 16 GB RAM, Docker Desktop with 7.7 GiB, 2026-10-15):

```bash
./scripts/measure_all.sh      # ~10-15 min; deletes existing pipeline data first
```

| What | Result |
|---|---|
| Model on held-out test set (40,000 txns, 917 fraud) | random forest, balanced: **precision 0.7727, recall 0.8899, F1 0.8272** |
| Same test set, logistic regression unweighted vs balanced | recall 0.5245 → 0.8997 (precision 0.8907 → 0.2293) |
| Live scoring, 19,026-txn backlog (labels vs flags) | precision 0.7856, recall 0.8887, F1 0.8340 |
| Live scoring, first ~3 min from cold start (3,773 txns) | precision 0.5619, recall 0.8939, F1 0.6901 |
| Processor throughput (backlog drain, start-up included) | **641 msg/s** (20,000 messages in 31.2 s) |
| `POST /score` latency, 1,000 sequential requests | **p50 1.56 ms, p95 1.76 ms**, p99 2.05 ms |
| Accounting | 3,982 raw = 3,773 stored + 209 quarantined; 209 of 209 dirty records caught (974 of 974 in the backlog) |
| SIGKILL of the processor | 0 lost, 0 duplicates; resumed after 46 s (Kafka session timeout 45 s) |
| Full replay of 23,982 messages | 0 new rows |
| Memory (long-running containers) | ≈ 827 MiB total (Kafka 575.5 MiB) |

Counts vary per run (the stream is random). The `PASS` checks must hold every time, and the model numbers
are identical on every Linux run. Details and caveats are in [LEARNING.md §7](LEARNING.md#7-measured-results-one-run).
## What it is

A local streaming pipeline in the payments domain, started with one command. A simulator produces
UPI-style transactions into Kafka, including labelled fraud patterns and deliberately dirty records.
A processor validates, cleans, deduplicates and **scores each transaction with an ML model**, then
stores the results in PostgreSQL, quarantining bad records with a reason. A FastAPI service exposes
health, scoring, stats and flagged transactions.

Study guide (walkthrough, concepts, exercises, interview Q&A): **[LEARNING.md](LEARNING.md)**.

## Run

```bash
docker compose up --build        # add -d to run in the background
```
This starts Kafka, creates the topics, trains the model (~10 s), then starts the producer, processor and API.
Optional: `cp .env.example .env` to change the rate, fraud share (`FRAUD_RATE`, default 2% of transactions),
number of users or DB credentials.

```bash
docker compose logs -f processor   # consumed / inserted / flagged / quarantined by reason / redeliveries
docker compose logs trainer        # the model's precision / recall / F1 report
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic transactions.flagged --property print.key=true
```

## API (interactive docs: http://localhost:8000/docs)

| Endpoint | What it does |
|---|---|
| `GET /health` | 200 if the model is loaded and the DB answers, else 503 |
| `POST /score` | Scores one transaction against the payer's stored history (read-only); 422 with a reason if invalid |
| `GET /stats` | Transactions, flagged, flag rate, rejected count by reason |
| `GET /transactions/flagged?limit=50` | Most recent flagged transactions |

```bash
curl localhost:8000/stats
curl -X POST localhost:8000/score -H 'content-type: application/json' -d '{"txn_id":"6f1c2b9e-1d2a-4c3b-9e8f-0a1b2c3d4e5f","payer_vpa":"priya.sharma1@oksbi","payee_vpa":"food.store1@ybl","amount":350.0,"timestamp":"2026-10-15T10:00:00+00:00","device_id":"dev-1","location":"Pune","merchant_category":"food"}'
```

PostgreSQL from your Mac: `postgresql://pipeline:pipeline@localhost:5433/upi`
(`docker compose exec postgres psql -U pipeline -d upi`).

## Test

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/ruff check .
docker compose up -d postgres      # DB tests use the real database; without it they are skipped
.venv/bin/pytest -v
```
**CI:** `.github/workflows/ci.yml` runs ruff and pytest on every push and pull request, against a Postgres
service container, with `REQUIRE_DB=1` so the DB tests fail rather than skip. (Validated with actionlint and
by running the same steps in a clean Linux container. It hasn't run on GitHub yet: no remote has been pushed.)

## Why recall matters for fraud

**Recall** is the share of real fraud that gets flagged. **Precision** is the share of flagged transactions
that really are fraud. A missed fraud is money lost and a victim. A false alarm costs a step-up check (OTP or
call). So the model is tuned for high recall, with precision kept high enough that alerts stay credible.
**Accuracy is misleading:** with ~2% fraud, a model that never flags anything is 97.71% accurate on the
test set and catches nothing. The trainer uses `class_weight="balanced"` and reports precision, recall and F1
on a time-ordered held-out set.
