# Unit tests for model/features.py: each feature, cold start, the 5-minute window, and the
# "only strictly earlier txns" rule that keeps training and serving consistent.

import math
from datetime import datetime, timedelta, timezone

import features

T = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def txn(amount=500.0, device="dev-a", ts=T):
    return {"timestamp": ts.isoformat(), "amount": amount, "device_id": device,
            "payer_vpa": "a.b1@oksbi", "payee_vpa": "x.y1@ybl"}


def past(seconds_ago, amount=500.0, device="dev-a", payee="p1@ybl"):
    return {"ts": T - timedelta(seconds=seconds_ago), "amount": amount, "device_id": device,
            "payee_vpa": payee}


def test_cold_start_has_neutral_values():
    f = features.compute_features(txn(), [])
    assert f == {"txn_count_5min": 0, "distinct_payees_5min": 0, "amount_vs_avg": 1.0,
                 "is_new_device": 0, "log_amount": math.log10(500.0),
                 "log_secs_since_last": features.NO_HISTORY_LOG_SECS, "history_count": 0}


def test_five_minute_window_boundary():
    hist = [past(240, payee="p1@ybl"), past(250, payee="p2@ybl"), past(360, payee="p3@ybl")]
    f = features.compute_features(txn(), hist)
    assert f["txn_count_5min"] == 2  # 6 minutes ago is outside the window
    assert f["distinct_payees_5min"] == 2
    assert f["history_count"] == 3  # but it still counts as history


def test_amount_vs_avg():
    f = features.compute_features(txn(amount=1000.0), [past(60, 100.0), past(120, 300.0)])
    assert f["amount_vs_avg"] == 5.0  # 1000 / mean(100, 300)


def test_new_device_only_with_history():
    assert features.compute_features(txn(device="dev-new"), [past(60)])["is_new_device"] == 1
    assert features.compute_features(txn(device="dev-a"), [past(60)])["is_new_device"] == 0


def test_seconds_since_last_txn():
    f = features.compute_features(txn(), [past(9), past(500)])
    assert f["log_secs_since_last"] == 1.0  # log10(9 + 1)


def test_self_and_future_txns_are_ignored():
    # A row with the same timestamp (the txn itself, on a redelivery) or a later one must not count.
    hist = [past(0), past(-5), past(30)]
    f = features.compute_features(txn(), hist)
    assert f["history_count"] == 1 and f["txn_count_5min"] == 1


def test_history_count_is_capped():
    f = features.compute_features(txn(), [past(1000 + i) for i in range(30)])
    assert f["history_count"] == features.HISTORY_COUNT_CAP


def test_only_most_recent_history_limit_txns_are_used():
    old = [past(10_000 + i, amount=1000.0) for i in range(50)]
    recent = [past(1000 + i, amount=100.0) for i in range(features.HISTORY_LIMIT)]
    f = features.compute_features(txn(amount=100.0), old + recent)
    assert f["amount_vs_avg"] == 1.0  # the 50 old ₹1000 txns fell outside the limit


def test_vector_follows_feature_names_order():
    f = features.compute_features(txn(), [past(60)])
    assert features.to_vector(f) == [float(f[name]) for name in features.FEATURE_NAMES]
