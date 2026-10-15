# FastAPI service over the pipeline's PostgreSQL: health check, score one transaction, stats, and
# recent flagged transactions. It reuses the processor's validation, history query, features and
# model, so POST /score gives the same answer the live pipeline would.
# Run locally: uvicorn app:app --app-dir api   (in compose: the `api` service on localhost:8000)

import json
import os
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, ConfigDict

import features
import scorer
import store
import validate

state = {}  # model bundle and DB pool, created once at startup (not per request)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Loading the model and opening DB connections is slow, so do it once when the app starts.
    state["bundle"] = scorer.load(os.getenv("MODEL_PATH", "model/model.joblib"))
    state["pool"] = ConnectionPool(
        os.getenv("DATABASE_URL", "postgresql://pipeline:pipeline@localhost:5433/upi"),
        min_size=1, max_size=4, timeout=5,  # wait at most 5 s for a connection, then fail
        open=True)
    yield
    state["pool"].close()


app = FastAPI(title="UPI fraud API", lifespan=lifespan)


@app.exception_handler(psycopg.OperationalError)  # also catches PoolTimeout (a subclass)
def database_unavailable(request: Request, exc: psycopg.OperationalError):
    # A database outage is "service unavailable, retry later" (503), not a bug in our code (500).
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


class Transaction(BaseModel):
    """Request body for POST /score: the same 8 fields the producer sends (no labels)."""
    # strict: "100" is NOT silently turned into 100.0, and true is not accepted as an amount.
    model_config = ConfigDict(strict=True)
    txn_id: str
    payer_vpa: str
    payee_vpa: str
    amount: float
    timestamp: str
    device_id: str
    location: str
    merchant_category: str


@app.get("/health")
def health():
    """200 if the model is loaded and the DB answers; 503 otherwise (for load balancers/Docker)."""
    try:
        with state["pool"].connection() as conn:
            conn.execute("SELECT 1")
    except Exception as exc:
        return JSONResponse(status_code=503, content={"status": "unhealthy", "error": str(exc)})
    return {"status": "ok", "model": state["bundle"]["name"]}


@app.post("/score")
def score(txn: Transaction):
    """Score one transaction against the payer's stored history. Does NOT store it."""
    # Same business rules as the pipeline, so the API can't accept what the pipeline would reject.
    clean, reason = validate.validate(json.dumps(txn.model_dump()).encode())
    if clean is None:
        raise HTTPException(status_code=422, detail={"reason": reason})

    with state["pool"].connection() as conn, conn.cursor() as cur:
        history = store.fetch_history(cur, clean["payer_vpa"], clean["timestamp"],
                                      features.HISTORY_LIMIT)
    feats = features.compute_features(clean, history)
    bundle = state["bundle"]
    probability = scorer.score(bundle, feats)
    return {"txn_id": clean["txn_id"], "fraud_score": round(probability, 4),
            "flagged": probability >= bundle["threshold"], "threshold": bundle["threshold"],
            "model": bundle["name"], "features": feats}


@app.get("/stats")
def stats():
    """Pipeline counters straight from the database."""
    with state["pool"].connection() as conn:
        total, flagged = conn.execute(
            "SELECT count(*), count(*) FILTER (WHERE flagged) FROM transactions").fetchone()
        by_reason = dict(conn.execute(
            "SELECT reason, count(*) FROM quarantine GROUP BY reason ORDER BY reason").fetchall())
    return {"transactions": total, "flagged": flagged,
            "flag_rate": round(flagged / total, 4) if total else 0.0,
            "rejected": sum(by_reason.values()), "rejected_by_reason": by_reason}


@app.get("/transactions/flagged")
def flagged_transactions(limit: int = Query(50, ge=1, le=500)):
    """Most recent flagged transactions, newest first (uses the partial index on flagged rows)."""
    with state["pool"].connection() as conn:
        rows = conn.execute(
            """
            SELECT txn_id, payer_vpa, payee_vpa, amount, ts, device_id, location,
                   merchant_category, fraud_score
            FROM transactions WHERE flagged ORDER BY ts DESC LIMIT %s
            """, (limit,)).fetchall()
    # Labels (is_fraud) are deliberately not returned: an alerts API wouldn't know them.
    return [{"txn_id": str(t), "payer_vpa": payer, "payee_vpa": payee, "amount": float(amount),
             "timestamp": ts.isoformat(), "device_id": dev, "location": loc,
             "merchant_category": cat, "fraud_score": score}
            for t, payer, payee, amount, ts, dev, loc, cat, score in rows]
