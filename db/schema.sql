-- Database schema, run automatically by the postgres image on its FIRST start
-- (files in /docker-entrypoint-initdb.d). To re-run it: docker compose down -v (deletes data).

-- Good, deduplicated transactions. txn_id as PRIMARY KEY is what makes dedup durable:
-- the database itself refuses a second row with the same id, even after a processor restart.
CREATE TABLE transactions (
    txn_id            UUID PRIMARY KEY,
    payer_vpa         TEXT NOT NULL,
    payee_vpa         TEXT NOT NULL,
    amount            NUMERIC(12, 2) NOT NULL CHECK (amount > 0),  -- NUMERIC, not float: money must be exact
    ts                TIMESTAMPTZ NOT NULL,
    device_id         TEXT NOT NULL,
    location          TEXT NOT NULL,
    merchant_category TEXT NOT NULL,
    is_fraud          BOOLEAN,  -- simulator ground truth, used only for training/evaluation
    fraud_pattern     TEXT,
    fraud_score       REAL,     -- model's probability of fraud (0..1), set by the processor
    flagged           BOOLEAN NOT NULL DEFAULT false,  -- fraud_score >= model threshold
    -- Where in Kafka this row came from. Lets us tell a Kafka redelivery (same offset)
    -- apart from a genuine duplicate sent twice (different offset).
    kafka_partition   INT NOT NULL,
    kafka_offset      BIGINT NOT NULL,
    processed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Features ask "this payer's recent txns before time t"; this index makes that fast.
CREATE INDEX transactions_payer_ts ON transactions (payer_vpa, ts);
-- GET /transactions/flagged reads only flagged rows, newest first. A partial index stores just those.
CREATE INDEX transactions_flagged_ts ON transactions (ts DESC) WHERE flagged;

-- Rejected records, kept with a reason instead of silently dropped, so they can be inspected or replayed.
CREATE TABLE quarantine (
    id              BIGSERIAL PRIMARY KEY,
    reason          TEXT NOT NULL,
    raw_message     TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- One Kafka message can only be quarantined once, even if Kafka delivers it again.
    UNIQUE (kafka_partition, kafka_offset)
);
