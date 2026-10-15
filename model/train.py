# Trains the fraud model on simulated, labelled transactions and reports precision, recall and F1
# on a held-out test set. Deterministic: the same arguments always give the same model and numbers.
# Run: python model/train.py --out model/model.joblib   (in compose: the one-shot `trainer` service)

import argparse
import json
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix, f1_score, precision_score, recall_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

import features
import generator

THRESHOLD = 0.5  # score >= this -> flagged


def make_dataset(n_txns: int, seed: int, fraud_rate: float, num_users: int, tps: float = 20.0):
    """Simulate a stream like the live producer's and compute features exactly like the processor.

    Dirty records are left out (dirty_rate=0) because online they are quarantined and never
    become part of anyone's history. Returns X, y, and the fraud pattern of each row.
    """
    world = generator.make_world(num_users, random.Random(seed))  # same population as the producer
    rng = random.Random(seed + 1)
    clock = datetime(2026, 1, 1, tzinfo=timezone.utc)
    step = timedelta(seconds=1 / tps)
    histories = defaultdict(list)  # payer_vpa -> earlier txns: the offline twin of the DB query
    X, y, patterns = [], [], []

    while len(y) < n_txns:
        _, records = generator.next_batch(world, rng, clock, fraud_rate, 0.0)
        for rec in records:
            hist = histories[rec["payer_vpa"]]
            X.append(features.to_vector(features.compute_features(rec, hist)))
            y.append(int(rec["is_fraud"]))
            patterns.append(rec["fraud_pattern"])
            hist.append({"ts": datetime.fromisoformat(rec["timestamp"]), "amount": rec["amount"],
                         "device_id": rec["device_id"], "payee_vpa": rec["payee_vpa"]})
        clock += step * len(records)  # the live producer sends one message per 1/tps seconds
    return np.array(X[:n_txns]), np.array(y[:n_txns]), patterns[:n_txns]


def evaluate(y_true, y_pred) -> dict:
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    return {
        "precision": round(precision_score(y_true, y_pred, zero_division=0), 4),
        "recall": round(recall_score(y_true, y_pred, zero_division=0), 4),
        "f1": round(f1_score(y_true, y_pred, zero_division=0), 4),
        "tp": int(tp), "fp": int(fp), "fn": int(fn), "tn": int(tn),
    }


def candidate_models(seed: int) -> dict:
    return {
        # Same model without class weights, to show what imbalance does to recall.
        "logreg_unweighted": make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000)),
        # class_weight="balanced": a missed fraud costs ~49x a false alarm in the loss, because
        # fraud is ~2% of the data. Without it, "predict not-fraud" is almost always right.
        "logreg_balanced": make_pipeline(
            StandardScaler(), LogisticRegression(max_iter=1000, class_weight="balanced")),
        "random_forest_balanced": RandomForestClassifier(
            n_estimators=100, min_samples_leaf=5, class_weight="balanced",
            random_state=seed, n_jobs=-1),
    }


def train(n_txns: int = 200_000, seed: int = 42, fraud_rate: float = 0.02,
          num_users: int = 2000) -> tuple[dict, dict]:
    X, y, patterns = make_dataset(n_txns, seed, fraud_rate, num_users)

    # Time-ordered split: train on the past, choose on the "next" period, test on the latest.
    # A random split would let the model learn from txns that happen after the ones it's tested
    # on, which can't happen in production.
    a, b = int(0.6 * len(y)), int(0.8 * len(y))
    X_tr, y_tr, X_val, y_val, X_te, y_te = X[:a], y[:a], X[a:b], y[a:b], X[b:], y[b:]

    report = {
        "n_txns": len(y), "fraud_share": round(float(y.mean()), 4),
        "train_rows": len(y_tr), "val_rows": len(y_val), "test_rows": len(y_te),
        "test_fraud_rows": int(y_te.sum()), "threshold": THRESHOLD,
        # Why not accuracy: predicting "not fraud" for everything scores this, yet catches nothing.
        "majority_baseline": {**evaluate(y_te, np.zeros_like(y_te)),
                              "accuracy": round(float((y_te == 0).mean()), 4)},
        "models": {},
    }
    fitted = {}
    for name, model in candidate_models(seed).items():
        model.fit(X_tr, y_tr)
        fitted[name] = model
        val = evaluate(y_val, (model.predict_proba(X_val)[:, 1] >= THRESHOLD).astype(int))
        test = evaluate(y_te, (model.predict_proba(X_te)[:, 1] >= THRESHOLD).astype(int))
        report["models"][name] = {"val": val, "test": test}

    # Pick by VALIDATION F1, never by test: choosing on the test set would make its numbers
    # optimistic.
    weighted = [n for n in fitted if n != "logreg_unweighted"]
    best = max(weighted, key=lambda n: report["models"][n]["val"]["f1"])
    report["selected"] = best
    model = fitted[best]
    if hasattr(model, "n_jobs"):
        # Serving scores one txn at a time; starting threads would cost more than it saves.
        model.set_params(n_jobs=1)

    # Recall per fraud pattern on the test set: shows WHICH kinds of fraud the model misses.
    pred_te = model.predict_proba(X_te)[:, 1] >= THRESHOLD
    per_pattern = {}
    for pattern in generator.FRAUD_PATTERNS:
        idx = [i for i, p in enumerate(patterns[b:]) if p == pattern]
        if idx:
            per_pattern[pattern] = {"rows": len(idx),
                                    "recall": round(float(pred_te[idx].mean()), 4)}
    report["test_recall_by_pattern"] = per_pattern
    if best.startswith("random_forest"):
        importances = [round(float(v), 4) for v in model.feature_importances_]
        report["feature_importance"] = dict(zip(features.FEATURE_NAMES, importances))

    bundle = {"model": model, "feature_names": features.FEATURE_NAMES, "threshold": THRESHOLD,
              "name": best}
    return bundle, report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="model/model.joblib")
    parser.add_argument("--n-txns", type=int, default=200_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    bundle, report = train(n_txns=args.n_txns, seed=args.seed)
    joblib.dump(bundle, args.out)
    print(json.dumps(report, indent=2))
    print(f"saved {bundle['name']} to {args.out}")


if __name__ == "__main__":
    main()
