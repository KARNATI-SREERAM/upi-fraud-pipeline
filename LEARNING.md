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
