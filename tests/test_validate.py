# Unit tests for processor/validate.py: one test per rule, plus a check that every kind of dirty
# record the Phase 1 generator produces is caught with the right reason.

import json
import random
from datetime import datetime, timedelta, timezone

import pytest

import generator
import validate

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def rng():
    return random.Random(1)


@pytest.fixture
def world(rng):
    return generator.make_world(100, rng)


@pytest.fixture
def good(world, rng):
    return generator.normal_txn(world, rng, NOW)


def check(rec):
    """Validate a dict the way it arrives from Kafka: as JSON bytes."""
    return validate.validate(json.dumps(rec).encode(), now=NOW)


def test_clean_record_passes(good):
    txn, reason = check(good)
    assert reason is None
    assert txn["txn_id"] == good["txn_id"]
    assert txn["amount"] == good["amount"]


@pytest.mark.parametrize("raw", [b"not json", b"[1, 2]", b'"a string"', None, b"\xff\xfe"])
def test_invalid_json(raw):
    assert validate.validate(raw, now=NOW) == (None, "invalid_json")


@pytest.mark.parametrize("field", validate.REQUIRED_FIELDS)
def test_missing_or_null_field(good, field):
    absent = {k: v for k, v in good.items() if k != field}
    assert check(absent) == (None, f"missing_field:{field}")
    assert check({**good, field: None}) == (None, f"missing_field:{field}")


def test_bad_txn_id(good):
    assert check({**good, "txn_id": "12345"}) == (None, "bad_txn_id")


def test_cleaning_normalises_vpa_case_and_whitespace(good):
    txn, reason = check({**good, "payer_vpa": "  " + good["payer_vpa"].upper() + " "})
    assert reason is None
    assert txn["payer_vpa"] == good["payer_vpa"]


@pytest.mark.parametrize("bad", ["rahulokaxis", "rahul@@okaxis", "", "@okaxis", "rahul x@okaxis",
                                 "rahul@ok-axis1"])
@pytest.mark.parametrize("which", ["payer", "payee"])
def test_malformed_vpa(good, bad, which):
    assert check({**good, f"{which}_vpa": bad}) == (None, f"malformed_vpa:{which}")


def test_self_payment(good):
    assert check({**good, "payee_vpa": good["payer_vpa"]}) == (None, "self_payment")


@pytest.mark.parametrize("amount", ["100", True, float("nan"), float("inf")])
def test_bad_amount(good, amount):
    assert check({**good, "amount": amount}) == (None, "bad_amount")


@pytest.mark.parametrize("amount", [0, -0.01, -500])
def test_non_positive_amount(good, amount):
    assert check({**good, "amount": amount}) == (None, "non_positive_amount")


@pytest.mark.parametrize("ts", ["yesterday", "2026-13-01T00:00:00+00:00", "2026-01-01T12:00:00"])
def test_bad_timestamp(good, ts):
    assert check({**good, "timestamp": ts}) == (None, "bad_timestamp")


def test_future_timestamp_beyond_skew_rejected_within_skew_ok(good):
    far = (NOW + timedelta(minutes=10)).isoformat()
    near = (NOW + timedelta(minutes=1)).isoformat()
    assert check({**good, "timestamp": far}) == (None, "future_timestamp")
    assert check({**good, "timestamp": near})[1] is None


EXPECTED_REASON_PREFIX = {
    "missing_field": "missing_field:",
    "malformed_vpa": "malformed_vpa:",
    "negative_amount": "non_positive_amount",
}


@pytest.mark.parametrize("dirty_type", generator.DIRTY_TYPES)
def test_every_generator_dirty_record_is_handled(world, rng, dirty_type):
    for _ in range(200):
        records = generator.make_dirty(dirty_type, generator.normal_txn(world, rng, NOW), rng)
        results = [check(r) for r in records]
        if dirty_type == "duplicate":
            # Both copies are individually valid; the database PK catches the second one.
            assert all(reason is None for _, reason in results)
        else:
            [(_, reason)] = results
            assert reason is not None and reason.startswith(EXPECTED_REASON_PREFIX[dirty_type])


def test_fraud_records_are_not_rejected(world, rng):
    # Cleaning must not throw away the interesting data: fraud is valid data, just suspicious.
    for pattern in generator.FRAUD_PATTERNS:
        for txn in generator.fraud_txns(pattern, world, rng, NOW):
            assert check(txn)[1] is None
