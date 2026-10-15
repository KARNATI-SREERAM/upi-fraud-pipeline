# Integration tests for processor/store.py against a REAL PostgreSQL (the one from docker compose).
# Uses the `cur` fixture from conftest.py: skipped if Postgres isn't reachable, rolled back after.

import uuid

import psycopg
import pytest

import store

TEST_PARTITION = 999  # a partition number real data never uses, so we can't collide with it


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


def test_fetch_history_returns_only_earlier_txns_newest_first(cur):
    payer = f"hist.{uuid.uuid4().hex[:8]}@testbank"
    for i, ts in enumerate(["2026-01-01T12:00:00+00:00", "2026-01-01T12:01:00+00:00",
                            "2026-01-01T12:02:00+00:00"]):
        store.save_txn(cur, {**make_txn(), "payer_vpa": payer, "timestamp": ts}, TEST_PARTITION, i)
    hist = store.fetch_history(cur, payer, "2026-01-01T12:02:00+00:00", limit=10)
    assert [h["ts"].minute for h in hist] == [1, 0]  # 12:02 itself excluded, newest first
    [latest] = store.fetch_history(cur, payer, "2026-01-01T12:02:00+00:00", limit=1)
    assert latest["ts"].minute == 1
