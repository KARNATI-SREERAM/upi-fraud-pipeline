# Loads the trained model bundle and scores one transaction's features.
# Used by the processor (live scoring) and the API (POST /score).

import joblib


def load(path: str) -> dict:
    """Bundle saved by train.py: {model, feature_names, threshold, name, ...}."""
    return joblib.load(path)


def score(bundle: dict, features: dict) -> float:
    """Probability (0..1) that this txn is fraud."""
    # Feature order must match training exactly, so we use the names saved inside the bundle.
    x = [[float(features[name]) for name in bundle["feature_names"]]]
    # predict_proba returns [[P(not fraud), P(fraud)]] for our one row; take P(fraud).
    return float(bundle["model"].predict_proba(x)[0][1])
