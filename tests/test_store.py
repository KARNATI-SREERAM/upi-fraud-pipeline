# Integration tests for processor/store.py against a REAL PostgreSQL (the one from docker compose).
# Skipped automatically if Postgres isn't reachable. Each test runs inside a transaction that is
# rolled back afterwards, so no test data is left behind.

import os
import uuid

import psycopg
import pytest

import store

DSN = os.getenv("TEST_DATABASE_URL", "postgresql://pipeline:pipeline@localhost:5433/upi")
TEST_PARTITION = 999  # a partition number real data never uses, so we can't collide with it


@pytest.fixture
def cur():
    try:
        conn = psycopg.connect(DSN, connect_timeout=2)
    except psycopg.OperationalError:
        pytest.skip(f"PostgreSQL not reachable at {DSN} (run `docker compose up -d postgres`)")
    with conn.cursor() as c:
        yield c
    conn.rollback()
    conn.close()


def make_txn():
    return {"txn_id": str(uuid.uuid4()), "payer_vpa": "a.b1@oksbi", "payee_vpa": "c.d2@ybl",
            "amount": 250.5, "timestamp": "2026-01-01T12:00:00+00:00", "device_id": "dev-1",
            "location": "Pune", "merchant_category": "food", "is_fraud": False,
            "fraud_pattern": None}


def test_first_insert(cur):
    assert store.save_txn(cur, make_txn(), TEST_PARTITION, 1) == "inserted"


def test_same_message_again_is_redelivery(cur):
    txn = make_txn()
    store.save_txn(cur, txn, TEST_PARTITION, 1)
    assert store.save_txn(cur, txn, TEST_PARTITION, 1) == "redelivery"


def test_same_txn_id_from_other_message_is_duplicate(cur):
    txn = make_txn()
    store.save_txn(cur, txn, TEST_PARTITION, 1)
    assert store.save_txn(cur, txn, TEST_PARTITION, 2) == "duplicate"
    cur.execute("SELECT count(*) FROM transactions WHERE txn_id = %s", (txn["txn_id"],))
    assert cur.fetchone() == (1,)  # still exactly one row


def test_amount_is_stored_exactly(cur):
    txn = {**make_txn(), "amount": 0.1 + 0.2}  # 0.30000000000000004 as a float
    store.save_txn(cur, txn, TEST_PARTITION, 1)
    cur.execute("SELECT amount::text FROM transactions WHERE txn_id = %s", (txn["txn_id"],))
    assert cur.fetchone() == ("0.30",)  # NUMERIC(12,2) stores exact money


def test_quarantine_is_idempotent(cur):
    assert store.save_quarantine(cur, "bad_amount", "{}", TEST_PARTITION, 7) == "inserted"
    assert store.save_quarantine(cur, "bad_amount", "{}", TEST_PARTITION, 7) == "redelivery"
    cur.execute("SELECT count(*) FROM quarantine WHERE kafka_partition = %s", (TEST_PARTITION,))
    assert cur.fetchone() == (1,)


def test_db_rejects_non_positive_amount(cur):
    # Defence in depth: even if validate.py had a bug, the CHECK constraint stops bad money data.
    with pytest.raises(psycopg.errors.CheckViolation):
        store.save_txn(cur, {**make_txn(), "amount": -1}, TEST_PARTITION, 1)
