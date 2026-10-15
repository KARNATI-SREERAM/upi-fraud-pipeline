# LEARNING.md: Project 1 study notes

<!-- Built up phase by phase. Final sections (full walkthrough, file table, 25 interview Qs) come in Phase 5. -->

---

## Phase 1: Producer + Kafka

### What was built
| File | Purpose | Key concept |
|---|---|---|
| `docker-compose.yml` | Starts Kafka, creates topics, starts the producer | Service dependencies, healthchecks, KRaft, listeners |
| `producer/generator.py` | Builds users/merchants and makes normal, fraud and dirty txns | Pure functions → easy to test; seeded randomness |
| `producer/main.py` | Sends generated txns to `transactions.raw` at a fixed rate | Message keys, delivery callbacks, `acks`, `flush()` |
| `producer/Dockerfile` | Container image for the producer | Layer caching, exec-form CMD (PID 1 gets SIGTERM) |
| `tests/test_generator.py` | 12 tests on the generator's output | Testing data shape without any infrastructure |
| `pyproject.toml` | ruff + pytest config | Linting, test discovery |

### Walk of one transaction
1. `main.py` calls `generator.next_batch()`. It rolls a random number: 2% fraud event, 5% dirty event, otherwise normal.
2. A normal txn picks a random user, a payee (70% merchant / 30% P2P), and an amount near that user's `typical_amount`.
3. `main.py` turns it into JSON and calls `producer.produce("transactions.raw", key=payer_vpa, value=...)`.
   This only puts it in a **local buffer**; it has not been sent yet.
4. The client batches buffered messages (`linger.ms=5`) and sends them to the broker.
5. Kafka hashes the key to choose a partition: `partition = hash(key) % 3`. The same payer always lands on the same partition.
6. The broker appends the message to the end of that partition's log and gives it the next **offset** (0, 1, 2, ...).
7. Because `acks=all`, the broker replies only once the write is stored. The client then runs `on_delivery()`, which increments `delivered`.

### Key concepts (tied to the code)
- **Topic.** A named stream of messages, like a table name for events. We have 3: `raw` (everything the producer sends), and `clean` / `flagged` for later phases. Created in `kafka-init`.
- **Partition.** A topic is split into partitions, and each one is an append-only log. Partitions are how Kafka scales: different consumers can read different partitions in parallel. You can see this in `kafka-topics.sh --describe` (PartitionCount: 3).
- **Offset.** A message's position inside one partition. Ordering is guaranteed **only within a partition**, not across the whole topic. `kafka-get-offsets.sh` showed `0:179 1:227 2:210`.
- **Message key → partition.** `main.py` uses `key=payer_vpa`, so all of one user's payments stay in order on one partition. Phase 2 needs this to compute "txns by this user in the last 5 min" correctly. Records with a missing payer get `key=None`, and Kafka spreads those across partitions.
- **Broker / KRaft.** The broker is the Kafka server that stores partitions. Older Kafka needed a separate ZooKeeper cluster for metadata. In KRaft mode Kafka manages metadata itself (`KAFKA_PROCESS_ROLES: broker,controller`), which means one fewer service and less RAM.
- **Listeners.** Clients first connect to a bootstrap address. The broker then tells them where to reconnect (the *advertised* listener). Containers must be told `kafka:9092` and your Mac must be told `localhost:9094`, so we define two listeners. Getting this wrong is the #1 "Kafka works in Docker but not from my laptop" bug.
- **acks=all + idempotence.** With `acks=all`, the broker confirms only after the write is durable. With `enable.idempotence=True`, a producer retry after a network blip can't write the same message twice. Note that this does **not** stop *our* deliberately duplicated records. Those have the same `txn_id` but are two separate `produce()` calls, so the processor must dedupe them in Phase 2.
- **flush() on shutdown.** `produce()` is asynchronous. If the process exits with messages still buffered, they are lost. `handle_stop()` catches SIGTERM so the loop can exit and `flush()` can drain the buffer. Measured: `sent=3715 delivered=3715 undelivered=0` after `docker compose stop producer`.
- **Labels vs features.** `is_fraud` / `fraud_pattern` are ground truth, used only to train and evaluate the model. If the scorer read them, it would look 100% accurate and be useless in production (**label leakage**).

### Observation to remember for Phase 3
Fraud *events* are 2%, but fraud bursts contain many txns (`many_payees` sends 8–15). In a sample of
1500 real messages, 220 were labelled fraud (≈14.7%). Real UPI fraud rates are far lower (well under 1%).
We'll decide in Phase 3 whether to lower `FRAUD_RATE` to get a realistic class imbalance.

### Break it to learn it
1. **Remove the message key.** In `main.py` change `key=key` to `key=None`, rebuild, and consume with `--property print.partition=true`.
   *What goes wrong:* the same user's txns now appear on different partitions.
   *Why:* without a key, the client spreads messages across partitions for balance. A consumer reading partition 0 no longer sees the user's full history, so per-user features (Phase 2) would be wrong or, with several consumers, split across machines.
2. **Break the advertised listener.** Change `EXTERNAL://localhost:9094` to `EXTERNAL://kafka:9094` and run the Mac consumer snippet against `localhost:9094`.
   *What goes wrong:* the first connection succeeds, then the client hangs or errors with "Failed to resolve kafka:9094".
   *Why:* bootstrap only returns metadata. The client then reconnects to the *advertised* address, and your Mac can't resolve the Docker-internal hostname `kafka`.
3. **Kill the broker mid-stream.** Run `docker compose stop kafka` for 20 s, then `docker compose start kafka`, while watching `docker compose logs -f producer`.
   *What goes wrong:* `delivered` stops increasing while `sent` keeps growing, and the gap is the messages sitting in the local buffer.
   *Why:* `produce()` only buffers locally, and librdkafka keeps retrying. If the outage is longer than `message.timeout.ms` (default 5 min), those messages fail, and `failed` goes up. When the broker returns, the buffer drains and `delivered` catches up. Try `docker compose kill producer` (SIGKILL, no flush) during an outage and compare `sent` with the offsets: the buffered messages are lost.

---

## Phase 2: Processor (validate, clean, dedup, quarantine → PostgreSQL)

### What was built
| File | Purpose | Key concept |
|---|---|---|
| `db/schema.sql` | `transactions` and `quarantine` tables | Primary key as durable dedup; `UNIQUE(partition, offset)`; NUMERIC for money; CHECK constraint |
| `processor/validate.py` | Turns one raw message into a clean txn, or a rejection reason | Validate vs clean vs reject; pure functions |
| `processor/store.py` | Idempotent inserts; tells "redelivery" apart from "duplicate" | `INSERT ... ON CONFLICT DO NOTHING`, `rowcount` |
| `processor/main.py` | Consumer loop: batch → DB commit → forward to clean → commit offsets | Consumer groups, manual offset commits, at-least-once |
| `tests/test_validate.py` | One test per rule + every generator dirty type is caught | Testing the contract between two services |
| `tests/test_store.py` | Runs against real Postgres, rolls back after each test | Integration tests, skip when infrastructure is missing |

### Walk of one message through the processor
1. `consumer.consume(100, timeout=1.0)` returns up to 100 messages from the partitions assigned to this consumer.
2. For each message, `validate.validate(msg.value())` parses the JSON, checks the 8 required fields, cleans the VPAs (strip + lowercase), then checks VPA format, self-payment, amount and timestamp. The first failure becomes the reason, e.g. `malformed_vpa:payer`.
3. If it's **rejected**, `store.save_quarantine()` writes the raw message + reason + (partition, offset).
4. If it's **valid**, `store.save_txn()` runs `INSERT ... ON CONFLICT (txn_id) DO NOTHING`.
   - If 1 row was written → `inserted`.
   - If 0 rows were written, the txn_id exists already. If the stored (partition, offset) matches this message, it's a `redelivery` (Kafka sent the same message again), so skip it. Otherwise it's a `duplicate` (a different message with the same id), so quarantine it as `duplicate_txn_id`.
5. After the whole batch, `conn.commit()` makes all of its rows durable at once.
6. Inserted txns are produced to `transactions.clean`, and `producer.flush()` waits until Kafka has them.
7. `consumer.commit()` tells Kafka "group `processor` has finished up to here". Only now will a restart skip these messages.

### Key concepts (tied to the code)
- **Consumer group.** `group.id: "processor"` in `main.py`. All consumers with the same group id share a topic's partitions, and each partition goes to exactly one member. With 3 partitions, up to 3 processors can work in parallel, and a 4th would sit idle. A *different* group id gets its own full copy of the stream (that's how the API or an analytics job could read the same data independently).
- **Offsets and commits.** Kafka doesn't delete a message when you read it. Instead, each group stores a *committed offset* per partition, meaning "read up to here". `enable.auto.commit=False` means *we* decide when to commit: only after the data is safe in Postgres. You can see this with `kafka-consumer-groups.sh --describe --group processor` (CURRENT-OFFSET, LOG-END-OFFSET, LAG).
- **At-least-once.** Commit order: DB first, offsets last. If we crash in between, Kafka re-sends those messages, so you may see a message **more than once but never zero times**. The alternative, **at-most-once** (commit offsets first), loses data on a crash. For payments, losing a transaction is worse than seeing it twice, provided the second time is harmless.
- **Idempotent writes (why dedup is needed).** At-least-once only works if processing a message twice has the same effect as once. The `txn_id` PRIMARY KEY + `ON CONFLICT DO NOTHING` does that for good records, and `UNIQUE(kafka_partition, kafka_offset)` does it for quarantined ones. Measured (see *Measured results* below): a full replay of 3159 messages gave `redeliveries=3159` and **0** new rows.
- **Redelivery vs duplicate.** These are two different problems that look alike. A *redelivery* is the same Kafka message again, with the same offset. A *duplicate* is two separate messages with the same txn_id (e.g. the sender's app retried). Storing (partition, offset) on each row is how the code tells them apart.
- **Exactly-once?** Kafka has transactions for Kafka-to-Kafka exactly-once, but here we write to Postgres, which is outside Kafka. The practical pattern is *at-least-once delivery + idempotent processing = effectively-once results*. The DB table is effectively-once. `transactions.clean` is at-least-once: after the replay it held 5992 messages for 2996 rows, so its readers must dedupe by txn_id too. A crash can cause the same effect on a smaller scale. If the kill lands after the DB commit but before the offset commit, the restarted processor re-forwards that batch. In the measured run the kill didn't land there (`redeliveries=0` after the restart).
- **Validate vs clean vs quarantine.** *Clean* what is safe to fix without guessing: VPA case and whitespace. *Reject* what you can't fix: a missing amount can't be invented. *Quarantine* rather than drop, so that bad data is visible, countable (the `/stats` endpoint in Phase 4 will use it) and replayable after a bug fix. Note that fraud is **not** dirty data: `test_fraud_records_are_not_rejected` makes sure cleaning never throws away the suspicious-but-valid records the model needs.
- **Defence in depth.** `CHECK (amount > 0)` in the schema stops bad money data even if `validate.py` has a bug (`test_db_rejects_non_positive_amount`). NUMERIC(12,2) stores money exactly: `0.1 + 0.2` is stored as `0.30`, not `0.30000000000000004`.
- **Crash-only design.** `main.py` has no clever partial-recovery code. Any exception exits the process, Docker restarts it (`restart: unless-stopped`), and Kafka replays the uncommitted batch. The DB transaction rolls back, so a half-written batch never exists.
- **Session timeout (seen in the crash test).** After `docker compose kill processor`, processing resumed only 46 s after the kill, even though the new processor started 5 s after it. The killed consumer never said goodbye, so the group waited for its `session.timeout.ms` (45 s default) to expire before giving its partitions to the new member. A graceful `docker compose stop` calls `consumer.close()`, which leaves the group at once, so there's no wait.

### Bug found during Phase 2 (a good interview story)
The Phase 1 producer seeded *everything* with `SEED=42`, including txn_ids. When the producer was restarted, it re-sent the same txn_ids from the start, and the processor correctly quarantined them all as duplicates. The fix is in `producer/main.py`: the seed now only fixes the **user population**, and the stream uses fresh randomness on every run. `scripts/verify_phase2.sh` restarts the producer mid-run and checks this: injected dirty events = quarantined records exactly (163 = 163 in the measured run). Any re-sent txn_id would show up as an extra `duplicate_txn_id`.

### Measured results (one run, one command)
Every Phase 2 number in this file and in README.md comes from this single run (MacBook, Apple Silicon,
Docker Desktop with 7.7 GiB, 2026-10-15). To reproduce it from the project folder:

```bash
./scripts/verify_phase2.sh      # about 4 minutes; WARNING: deletes existing pipeline data first
```

What the script does, in order:
1. `docker compose down -v`, then `up --build`: empty Kafka and Postgres, run for 60 s.
2. `docker compose kill processor` (SIGKILL), `start` it 5 s later. Times how long until a new offset is committed.
3. `docker compose restart producer`: checks that a restart doesn't re-send old txn_ids.
4. `docker stats`: memory while everything runs.
5. Stops the producer, waits for consumer lag 0, then checks the accounting (`PASS`/`FAIL` lines).
6. Stops the processor, resets the group's offsets to the start (`--reset-offsets --to-earliest`), replays everything, and checks that no rows were added.

Output of the measured run, unedited:
```
== 1. Clean start (fresh Kafka + Postgres), run for 60 s
== 2. SIGKILL the processor (no clean shutdown), restart it 5 s later
seconds from SIGKILL until processing resumed: 46
== 3. Restart the producer (a restart must not re-send old txn_ids)
== 4. Memory, all containers running
processor-1: 25.63MiB / 7.748GiB
producer-1: 10.2MiB / 7.748GiB
postgres-1: 29.06MiB / 7.748GiB
kafka-1: 478.6MiB / 7.748GiB
== 5. Stop the producer, wait for the processor to catch up (lag 0)
producer runs: 2, last reported rate: rate=20.0 txn/s
PASS  producer sent == messages in transactions.raw: 3159 = 3159
PASS  raw messages == transactions + quarantine: 3159 = 3159
PASS  dirty events injected == records quarantined: 163 = 163
PASS  duplicate txn_ids in transactions table: 0 = 0
transactions=2996 quarantine=163
quarantine by reason: duplicate_txn_id=42 malformed_vpa=49 missing_field=42 non_positive_amount=30
fraud-labelled transactions: 239 of 2996 (8.0%)
== 6. Full replay: reset the group's offsets to the start and reprocess everything
processor counters after the SIGKILL restart: consumed=1958 inserted=1860 quarantined=98 redeliveries=0
processor counters for the replay: consumed=3159 inserted=0 quarantined=0 redeliveries=3159
PASS  transactions after replay (unchanged): 2996 = 2996
PASS  quarantine after replay (unchanged): 163 = 163
transactions.clean messages: before replay=2996 after replay=5992
== Done (stack stopped, data volume kept)
```

How to read it:
- **3159 = 2996 + 163**: every raw message ended up in exactly one table. Nothing was lost and nothing was stored twice, despite the SIGKILL.
- **163 = 163**: every injected dirty record was caught, and no clean record was rejected. Of the 163, 42 are `duplicate_txn_id` (genuine duplicates, caught by the primary key, not by `validate.py`).
- **46 s**: the restarted processor sat idle until the dead consumer's session timed out (45 s).
- **redeliveries=3159, rows unchanged**: replaying the whole topic is harmless, because the writes are idempotent.
- **2996 → 5992 on `transactions.clean`**: the clean topic is at-least-once. Each replay re-forwards every record.
- Counts change on every run (the stream is random). The `PASS` lines must hold every time.

### Break it to learn it
1. **Commit offsets before the DB write (at-most-once).** In `main.py`, move `consumer.commit(asynchronous=False)` to just after `consume()`, then add `raise RuntimeError("boom")` after `process_batch(...)` and rebuild.
   *What goes wrong:* the accounting check fails, with fewer rows than raw messages.
   *Why:* offsets were committed for a batch whose DB transaction rolled back. On restart, Kafka skips it, so those records are lost forever. This is exactly why the commit order matters.
2. **Remove idempotency.** In `db/schema.sql`, change `txn_id UUID PRIMARY KEY` to `txn_id UUID NOT NULL` and remove `ON CONFLICT (txn_id) DO NOTHING` in `store.py`. Then `docker compose down -v && docker compose up --build -d` and do the offset reset replay (`kafka-consumer-groups.sh --group processor --topic transactions.raw --reset-offsets --to-earliest --execute` with the processor stopped).
   *What goes wrong:* every transaction now appears twice. `SELECT count(*) - count(DISTINCT txn_id) FROM transactions` is no longer 0.
   *Why:* at-least-once delivery without idempotent writes means duplicates. The database constraint is what turned "delivered twice" into "stored once".
3. **Run two processors.** `docker compose up -d --scale processor=2`, then `kafka-consumer-groups.sh --describe --group processor`.
   *What you'll see:* the 3 partitions split between two consumer IDs (2 + 1). Try `--scale processor=4`: one consumer gets no partition and sits idle.
   *Why:* within a group, a partition is owned by exactly one consumer. That keeps per-key ordering, but it means partitions cap your parallelism. Also notice that dedup still works across both processors, because it lives in the shared database, not in either process's memory.

---

## Phase 3: Features, model training, live scoring

### What was built
| File | Purpose | Key concept |
|---|---|---|
| `model/features.py` | 7 features from a txn + the payer's earlier txns | Feature engineering; one function for training AND serving (no train/serve skew) |
| `model/train.py` | Simulates 200k labelled txns, trains 3 models, reports P/R/F1 on a held-out test set | Time-ordered split, class weights, model selection on validation |
| `model/scorer.py` | Loads the model bundle, scores one txn | Model serving; fixed feature order |
| `model/Dockerfile` + `trainer` service | Trains at `docker compose up` into a shared volume | One-shot jobs, `service_completed_successfully` |
| `processor/pipeline.py` (was `main.py`) | Scores every valid txn, stores score + flag, sends flagged ones to `transactions.flagged` | Online feature lookup inside the batch's DB transaction |
| `producer/generator.py` | `FRAUD_RATE` now means "share of transactions" (2%) | Class imbalance you control |

### Key concepts
- **Class imbalance.** About 2% of txns are fraud. A model that always says "not fraud" is ~98% *accurate* and catches nothing, which is why the trainer reports precision, recall and F1 and prints that majority baseline explicitly.
- **Precision vs recall.** Precision is "of the txns we flagged, how many were fraud". Recall is "of the fraud, how much did we flag". For fraud, **recall matters most**: a missed fraud is lost money and a victim, while a false alarm costs an extra check (an OTP or a call). Precision still matters, because too many false alarms and users or ops teams start ignoring them.
- **Class weights.** `class_weight="balanced"` makes each fraud row count ~49× more in the loss (98/2). The trainer shows the effect directly: the same logistic regression with and without weights.
- **Time-ordered split.** Train on the first 60% of time, choose the model on the next 20%, report on the last 20%. A random split would let the model "see the future".
- **No train/serve skew.** `compute_features(txn, history)` is the only feature code. Offline the history is an in-memory list, and online it's `store.fetch_history` (a SQL query using the `(payer_vpa, ts)` index). Both pass "this payer's earlier txns, `ts <` this one, newest 100".
- **Two modelling bugs found and fixed while building** (good interview stories):
  1. **Feature drift over time.** An uncapped `history_count` keeps growing, so the latest (test) period had values the forest never saw in training. Test recall came out far below validation recall. Trees can't extrapolate beyond the training range. The fix is to cap it at 10.
  2. **A missing signal.** `many_payees` fraud was almost never caught. Normal users also pay a different payee each time, so the 5-minute counts alone can't tell a burst from normal activity. Adding `log_secs_since_last` (bursts are 0.1–0.9 s apart, normal users pay every few minutes) fixed it, and it became the most important feature (see `feature_importance` in the measured run).

### Results
The exact numbers are in the **Measured run** section (Phase 5), produced by `docker compose logs trainer` in that run.

### Break it to learn it
1. **Remove the class weights.** In `train.py`, delete `class_weight="balanced"` from the random forest and retrain (`PYTHONPATH=producer python model/train.py`).
   *What happens:* test recall drops and precision rises.
   *Why:* without weights the loss is dominated by the 98% non-fraud rows, so the cheapest way to lower it is to say "not fraud" more often.
2. **Use a random split.** Shuffle `X, y` before splitting.
   *What happens:* validation and test scores move closer together. In this simulation the change is small, because the data's behaviour doesn't change over time.
   *Why:* the model now trains on rows from the same period it's tested on, including other txns from the same fraud bursts. That's leakage: production never gives you tomorrow's data. With real data, where fraud tactics change month to month, a random split can make a model look far better than it will perform.
3. **Create train/serve skew.** Change `h["ts"] < ts` to `h["ts"] <= ts` in `features.compute_features` **and** `ts < %s` to `ts <= %s` in `store.fetch_history`. Then rebuild, and replay (reset the processor's offsets, as in Phase 2).
   *What happens:* training doesn't change, but replayed txns get different features and scores than on the first pass (e.g. `txn_count_5min` is one higher).
   *Why:* offline, a txn is appended to history only *after* it's scored, so it never sees itself. Online, on a replay the txn is already in the DB, and `<=` counts it as its own history. The same code with different data timing is the classic train/serve skew.
