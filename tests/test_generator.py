# Unit tests for producer/generator.py: checks the shape of normal, fraud and dirty transactions.
# No Kafka needed; the generator is pure Python on purpose.

import random
import re
from datetime import datetime, timezone

import pytest

import generator

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
VPA_RE = re.compile(r"^[a-z0-9._-]+@[a-z]+$")


@pytest.fixture
def rng():
    return random.Random(42)


@pytest.fixture
def world(rng):
    return generator.make_world(100, rng)


def user_of(world, vpa):
    return next(u for u in world.users if u.vpa == vpa)


def test_normal_txn_has_all_fields_and_valid_vpas(world, rng):
    for _ in range(200):
        txn = generator.normal_txn(world, rng, NOW)
        assert set(generator.REQUIRED_FIELDS) <= txn.keys()
        assert VPA_RE.match(txn["payer_vpa"])
        assert VPA_RE.match(txn["payee_vpa"])
        assert txn["payer_vpa"] != txn["payee_vpa"]
        assert txn["amount"] > 0
        assert txn["is_fraud"] is False


def test_rapid_repeat_same_payee_many_times(world, rng):
    txns = generator.fraud_txns("rapid_repeat", world, rng, NOW)
    assert 5 <= len(txns) <= 8
    assert len({t["payee_vpa"] for t in txns}) == 1
    assert len({t["txn_id"] for t in txns}) == len(txns)  # each is a distinct real payment
    assert all(t["is_fraud"] and t["fraud_pattern"] == "rapid_repeat" for t in txns)


def test_large_amount_is_at_least_10x_typical(world, rng):
    [txn] = generator.fraud_txns("large_amount", world, rng, NOW)
    user = user_of(world, txn["payer_vpa"])
    assert txn["amount"] >= 10 * user.typical_amount - 0.01  # tolerance for rounding
    assert txn["device_id"] in user.devices


def test_new_device_large_uses_unseen_device(world, rng):
    [txn] = generator.fraud_txns("new_device_large", world, rng, NOW)
    user = user_of(world, txn["payer_vpa"])
    assert txn["device_id"] not in user.devices
    assert txn["amount"] >= 5 * user.typical_amount - 0.01


def test_many_payees_fans_out(world, rng):
    txns = generator.fraud_txns("many_payees", world, rng, NOW)
    assert len({t["payee_vpa"] for t in txns}) >= 8
    assert len({t["payer_vpa"] for t in txns}) == 1


@pytest.mark.parametrize("dirty_type", generator.DIRTY_TYPES)
def test_every_dirty_type_is_actually_dirty(dirty_type, world, rng):
    clean = generator.normal_txn(world, rng, NOW)
    out = generator.make_dirty(dirty_type, clean, rng)
    if dirty_type == "missing_field":
        assert not set(generator.REQUIRED_FIELDS) <= out[0].keys()
    elif dirty_type == "duplicate":
        assert len(out) == 2 and out[0]["txn_id"] == out[1]["txn_id"]
    elif dirty_type == "malformed_vpa":
        vpas = [out[0]["payer_vpa"], out[0]["payee_vpa"]]
        assert not all(VPA_RE.match(v) for v in vpas)
    elif dirty_type == "negative_amount":
        assert out[0]["amount"] < 0


def test_make_dirty_does_not_mutate_input(world, rng):
    clean = generator.normal_txn(world, rng, NOW)
    snapshot = dict(clean)
    for dirty_type in generator.DIRTY_TYPES:
        generator.make_dirty(dirty_type, clean, rng)
    assert clean == snapshot


def test_same_seed_gives_same_stream():
    def stream(seed):
        rng = random.Random(seed)
        world = generator.make_world(50, rng)
        return [generator.next_batch(world, rng, NOW, 0.1, 0.1) for _ in range(100)]

    assert stream(7) == stream(7)
    assert stream(7) != stream(8)


def test_rates_roughly_respected(world, rng):
    kinds = [generator.next_batch(world, rng, NOW, 0.1, 0.2)[0] for _ in range(5000)]
    fraud = sum(k.startswith("fraud") for k in kinds) / len(kinds)
    dirty = sum(k.startswith("dirty") for k in kinds) / len(kinds)
    assert 0.08 < fraud < 0.12
    assert 0.17 < dirty < 0.23
