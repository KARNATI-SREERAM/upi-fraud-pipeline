# Integration test for processor/pipeline.py's process_batch: fake Kafka messages in, real
# PostgreSQL rows out (rolled back afterwards). A stub model makes scores predictable.

import json
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

import features
import pipeline

TEST_PARTITION = 999
COUNT_IDX = features.FEATURE_NAMES.index("txn_count_5min")
LOG_AMOUNT_IDX = features.FEATURE_NAMES.index("log_amount")


class StubModel:
    """Fraud probability = txn_count_5min / 10, or 0.9 for amounts >= ₹10,000."""

    def predict_proba(self, X):
        row = X[0]
        p = max(min(row[COUNT_IDX] / 10, 1.0), 0.9 if row[LOG_AMOUNT_IDX] >= 4 else 0.0)
        return [[1 - p, p]]


BUNDLE = {"model": StubModel(), "feature_names": features.FEATURE_NAMES, "threshold": 0.5,
          "name": "stub"}


class FakeMsg:
    """Just the parts of a confluent_kafka Message that process_batch uses."""

    def __init__(self, value, offset, partition=TEST_PARTITION):
        self._value, self._offset, self._partition = value, offset, partition

    def error(self):
        return None

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition


def make_txn(payer, amount=250.0, seconds_ago=60):
    ts = datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
    return {"txn_id": str(uuid.uuid4()), "payer_vpa": payer, "payee_vpa": "shop.one@ybl",
            "amount": amount, "timestamp": ts.isoformat(), "device_id": "dev-1",
            "location": "Pune", "merchant_category": "food", "is_fraud": False,
            "fraud_pattern": None}


def msg(txn, offset):
    return FakeMsg(json.dumps(txn).encode(), offset)


def new_payer():
    # A unique VPA, so the history query can't pick up real rows from a running pipeline.
    return f"t{uuid.uuid4().hex[:10]}@testbank"


def test_batch_inserts_quarantines_dedupes_and_detects_redelivery(cur):
    good = make_txn(new_payer())
    batch = [msg(good, 1),
             msg({**make_txn(new_payer()), "amount": -5}, 2),  # dirty
             msg(good, 3),                                    # same txn_id, new offset: duplicate
             msg(good, 1)]                                    # same message again: redelivery
    counts = Counter()
    forwarded = pipeline.process_batch(batch, cur, counts, BUNDLE)

    assert counts["inserted"] == 1
    assert counts["quarantined:non_positive_amount"] == 1
    assert counts["quarantined:duplicate_txn_id"] == 1
    assert counts["redeliveries"] == 1
    assert [t["txn_id"] for t in forwarded] == [good["txn_id"], good["txn_id"]]


def test_rows_are_stored_with_score_and_flag(cur):
    big = make_txn(new_payer(), amount=20_000.0)
    small = make_txn(new_payer(), amount=100.0)
    forwarded = pipeline.process_batch([msg(big, 10), msg(small, 11)], cur, Counter(), BUNDLE)
    assert [t["flagged"] for t in forwarded] == [True, False]
    cur.execute("SELECT fraud_score, flagged FROM transactions WHERE txn_id = %s", (big["txn_id"],))
    score, flagged = cur.fetchone()
    assert abs(score - 0.9) < 1e-6 and flagged is True


def test_earlier_txns_in_the_same_batch_count_as_history(cur):
    payer = new_payer()
    txns = [make_txn(payer, seconds_ago=s) for s in (30, 20, 10)]
    forwarded = pipeline.process_batch([msg(t, 20 + i) for i, t in enumerate(txns)],
                                       cur, Counter(), BUNDLE)
    # Third txn sees the two earlier ones (same DB transaction): count 2 -> stub score 0.2.
    assert [t["fraud_score"] for t in forwarded] == [0.0, 0.1, 0.2]
