# Feature engineering for fraud scoring, shared by training (model/train.py) and serving
# (processor/pipeline.py and the API). Using ONE function in both places prevents train/serve skew:
# the model sees features computed exactly the same way it was trained on.

import math
from datetime import datetime, timedelta

FEATURE_NAMES = ["txn_count_5min", "distinct_payees_5min", "amount_vs_avg", "is_new_device",
                 "log_amount", "log_secs_since_last", "history_count"]
WINDOW = timedelta(minutes=5)
# Only the payer's most recent N earlier txns are used. That keeps the per-txn DB query small
# (LIMIT in store.fetch_history) and makes offline and online see the same history.
HISTORY_LIMIT = 100
# history_count is capped: an uncapped count keeps growing over time, so newer data has values
# the model never saw in training (trees can't extrapolate). After 10 txns, "enough history".
HISTORY_COUNT_CAP = 10
# log10 of seconds since the last txn when there is none: 6 ~ 11 days, i.e. "a long time ago".
NO_HISTORY_LOG_SECS = 6.0


def _as_datetime(ts) -> datetime:
    return ts if isinstance(ts, datetime) else datetime.fromisoformat(ts)


def compute_features(txn: dict, history: list[dict]) -> dict:
    """Features for one txn, given this payer's previously processed txns.

    history items need: ts (datetime), amount, device_id, payee_vpa.
    Only items strictly EARLIER than this txn count, so a txn never sees itself or the future.
    """
    ts = _as_datetime(txn["timestamp"])
    past = sorted((h for h in history if h["ts"] < ts), key=lambda h: h["ts"])[-HISTORY_LIMIT:]
    recent = [h for h in past if h["ts"] >= ts - WINDOW]
    amount = float(txn["amount"])
    avg = sum(float(h["amount"]) for h in past) / len(past) if past else None

    return {
        # Rapid repeats and fan-out both show up as many txns in a short window.
        "txn_count_5min": len(recent),
        # Separates fan-out (many different payees) from rapid repeats (one payee).
        "distinct_payees_5min": len({h["payee_vpa"] for h in recent}),
        # 1.0 when there's no history: "no evidence it's unusual", rather than a made-up number.
        "amount_vs_avg": amount / avg if avg else 1.0,
        # Only meaningful with history: a brand-new user's first device isn't "new".
        "is_new_device": int(bool(past) and txn["device_id"] not in {h["device_id"] for h in past}),
        # log10, so ₹100 -> 2, ₹10,000 -> 4. Amounts are right-skewed; logs make them easier
        # to learn.
        "log_amount": math.log10(amount),
        # Bursts are 0.1-0.9 s apart; a normal user pays every few minutes. The strongest
        # single signal for rapid_repeat and many_payees.
        "log_secs_since_last": (math.log10((ts - past[-1]["ts"]).total_seconds() + 1)
                                if past else NO_HISTORY_LOG_SECS),
        # Tells the model how much to trust amount_vs_avg and is_new_device.
        "history_count": min(len(past), HISTORY_COUNT_CAP),
    }


def to_vector(features: dict) -> list[float]:
    """Dict -> list in the fixed FEATURE_NAMES order the model was trained with."""
    return [float(features[name]) for name in FEATURE_NAMES]
