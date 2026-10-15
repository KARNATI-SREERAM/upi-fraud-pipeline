# Tests for api/app.py with FastAPI's TestClient, against the real PostgreSQL and a small model
# trained just for these tests. Rows the tests insert use partition 999 and are deleted afterwards.

import os
import uuid
from datetime import datetime, timedelta, timezone

import joblib
import pytest
from fastapi.testclient import TestClient

import features
import store
import train
from conftest import DSN, connect_or_skip

TEST_PARTITION = 999


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    conn = connect_or_skip()
    bundle, _ = train.train(n_txns=5000, seed=1)
    model_path = tmp_path_factory.mktemp("model") / "model.joblib"
    joblib.dump(bundle, model_path)
    os.environ["MODEL_PATH"], os.environ["DATABASE_URL"] = str(model_path), DSN

    import app  # imported here so the env vars above are set before startup reads them
    with TestClient(app.app) as c:  # the `with` runs the lifespan (loads model, opens pool)
        yield c

    conn.execute("DELETE FROM transactions WHERE kafka_partition = %s", (TEST_PARTITION,))
    conn.commit()
    conn.close()


def body(**overrides):
    ts = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    txn = {"txn_id": str(uuid.uuid4()), "payer_vpa": f"api{uuid.uuid4().hex[:8]}@testbank",
           "payee_vpa": "shop.two@ybl", "amount": 450.0, "timestamp": ts, "device_id": "dev-9",
           "location": "Delhi", "merchant_category": "grocery"}
    return {**txn, **overrides}


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"


def test_score_returns_probability_and_features_without_storing(client):
    b = body()
    r = client.post("/score", json=b)
    assert r.status_code == 200
    out = r.json()
    assert 0.0 <= out["fraud_score"] <= 1.0
    assert out["flagged"] == (out["fraud_score"] >= out["threshold"])
    assert list(out["features"]) == features.FEATURE_NAMES
    with connect_or_skip() as conn:
        n = conn.execute("SELECT count(*) FROM transactions WHERE txn_id = %s",
                         (b["txn_id"],)).fetchone()[0]
    assert n == 0  # scoring is read-only


def test_score_rejects_business_rule_violation_with_reason(client):
    r = client.post("/score", json=body(payer_vpa="no-at-sign"))
    assert r.status_code == 422
    assert r.json()["detail"]["reason"] == "malformed_vpa:payer"


@pytest.mark.parametrize("bad", [{"amount": "100"}, {"amount": True}, {"txn_id": None}])
def test_score_rejects_wrong_types(client, bad):
    assert client.post("/score", json=body(**bad)).status_code == 422


def test_score_uses_stored_history(client):
    # Store 3 txns for one payer seconds apart, then score a 4th: features must see them.
    with connect_or_skip() as conn, conn.cursor() as cur:
        payer = f"hist{uuid.uuid4().hex[:8]}@testbank"
        now = datetime.now(timezone.utc)
        for i in range(3):
            t = body(payer_vpa=payer, timestamp=(now - timedelta(seconds=40 - i)).isoformat())
            store.save_txn(cur, {**t, "is_fraud": None, "fraud_pattern": None}, TEST_PARTITION,
                           10_000 + i)
        conn.commit()
    r = client.post("/score", json=body(payer_vpa=payer, timestamp=now.isoformat()))
    assert r.json()["features"]["txn_count_5min"] == 3


def test_stats_and_flagged(client):
    with connect_or_skip() as conn, conn.cursor() as cur:
        t = body()
        store.save_txn(cur, {**t, "is_fraud": None, "fraud_pattern": None, "fraud_score": 0.97,
                             "flagged": True}, TEST_PARTITION, 20_000)
        conn.commit()
    s = client.get("/stats").json()
    assert {"transactions", "flagged", "flag_rate", "rejected", "rejected_by_reason"} <= s.keys()
    assert s["flagged"] >= 1

    rows = client.get("/transactions/flagged", params={"limit": 500}).json()
    mine = [row for row in rows if row["txn_id"] == t["txn_id"]]
    assert mine and mine[0]["fraud_score"] == pytest.approx(0.97)
    assert "is_fraud" not in mine[0]  # labels are never exposed


def test_flagged_limit_is_validated(client):
    assert client.get("/transactions/flagged", params={"limit": 0}).status_code == 422


def test_database_outage_returns_503(client, monkeypatch):
    import app

    class DownPool:
        def connection(self):
            raise app.psycopg.OperationalError("connection refused")

    monkeypatch.setitem(app.state, "pool", DownPool())  # restored automatically after the test
    assert client.get("/stats").status_code == 503
    assert client.get("/health").status_code == 503
