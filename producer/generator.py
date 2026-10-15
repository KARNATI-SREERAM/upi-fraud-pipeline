# Generates realistic UPI-style transactions: normal ones, labelled fraud patterns,
# and dirty records.
# Kept free of any Kafka code so it can be unit-tested on its own (see tests/test_generator.py).

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta

FIRST_NAMES = ["priya", "rahul", "ananya", "arjun", "sneha", "vikram", "kavya", "rohan",
               "isha", "aditya", "meera", "karan", "divya", "siddharth", "pooja", "nikhil"]
LAST_NAMES = ["sharma", "verma", "reddy", "iyer", "patel", "nair", "gupta", "singh",
              "rao", "das", "joshi", "menon"]
# Real UPI handles look like these; the part after "@" identifies the bank/app (PSP).
HANDLES = ["okhdfcbank", "okaxis", "oksbi", "okicici", "ybl", "paytm"]
CITIES = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Kolkata",
          "Pune", "Ahmedabad", "Jaipur", "Lucknow"]
MERCHANT_CATEGORIES = ["grocery", "food", "fuel", "electronics", "travel", "utilities"]

FRAUD_PATTERNS = ["rapid_repeat", "large_amount", "new_device_large", "many_payees"]
DIRTY_TYPES = ["missing_field", "duplicate", "malformed_vpa", "negative_amount"]
REQUIRED_FIELDS = ["txn_id", "payer_vpa", "payee_vpa", "amount", "timestamp", "device_id",
                   "location", "merchant_category"]


@dataclass
class User:
    vpa: str
    city: str
    devices: list[str]
    typical_amount: float  # this user's "normal" spend; fraud patterns are defined relative to it


@dataclass
class Merchant:
    vpa: str
    category: str


@dataclass
class World:
    """Everything the generator needs: the fixed population of users and merchants."""
    users: list[User] = field(default_factory=list)
    merchants: list[Merchant] = field(default_factory=list)


def _new_id(rng: random.Random) -> str:
    # uuid.uuid4() uses the OS random source; building it from rng keeps output
    # reproducible by seed.
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _new_device(rng: random.Random) -> str:
    return f"dev-{rng.getrandbits(32):08x}"


def make_world(num_users: int, rng: random.Random, num_merchants: int = 50) -> World:
    users = []
    for i in range(num_users):
        name = f"{rng.choice(FIRST_NAMES)}.{rng.choice(LAST_NAMES)}{i}"  # index keeps VPAs unique
        users.append(User(
            vpa=f"{name}@{rng.choice(HANDLES)}",
            city=rng.choice(CITIES),
            devices=[_new_device(rng) for _ in range(rng.randint(1, 2))],
            # Log-normal: most people spend a few hundred rupees, a few spend much more
            # (right skew).
            # median = e^6 ≈ ₹400.
            typical_amount=round(rng.lognormvariate(6.0, 0.8), 2),
        ))
    merchants = []
    for i in range(num_merchants):
        category = rng.choice(MERCHANT_CATEGORIES)
        merchants.append(Merchant(vpa=f"{category}.store{i}@{rng.choice(HANDLES)}",
                                  category=category))
    return World(users=users, merchants=merchants)


def _txn(user: User, payee_vpa: str, category: str, amount: float, device: str,
         location: str, ts: datetime, rng: random.Random,
         is_fraud: bool = False, fraud_pattern: str | None = None) -> dict:
    return {
        "txn_id": _new_id(rng),
        "payer_vpa": user.vpa,
        "payee_vpa": payee_vpa,
        "amount": round(max(amount, 1.0), 2),
        "timestamp": ts.isoformat(),
        "device_id": device,
        "location": location,
        "merchant_category": category,
        # Ground-truth labels for training the model (Phase 3). The processor must never
        # use these to score a transaction; that would be label leakage.
        "is_fraud": is_fraud,
        "fraud_pattern": fraud_pattern,
    }


def _pick_payee(world: World, payer: User, rng: random.Random) -> tuple[str, str]:
    """Return (payee_vpa, category). 70% merchant payments, 30% person-to-person (P2P)."""
    if rng.random() < 0.7:
        m = rng.choice(world.merchants)
        return m.vpa, m.category
    while True:
        other = rng.choice(world.users)
        if other.vpa != payer.vpa:
            return other.vpa, "p2p"


def normal_txn(world: World, rng: random.Random, now: datetime) -> dict:
    user = rng.choice(world.users)
    payee, category = _pick_payee(world, user, rng)
    amount = user.typical_amount * rng.lognormvariate(0, 0.5)  # varies around the user's usual
    return _txn(user, payee, category, amount, rng.choice(user.devices), user.city, now, rng)


def _burst_times(now: datetime, n: int, rng: random.Random) -> list[datetime]:
    """n timestamps 0.1–0.9 s apart, which is how scripted/bot fraud looks."""
    times, t = [], now
    for _ in range(n):
        times.append(t)
        t += timedelta(seconds=rng.uniform(0.1, 0.9))
    return times


def fraud_txns(pattern: str, world: World, rng: random.Random, now: datetime) -> list[dict]:
    user = rng.choice(world.users)
    device = rng.choice(user.devices)
    label = {"is_fraud": True, "fraud_pattern": pattern}

    if pattern == "rapid_repeat":
        # Same payer -> same payee, many times in seconds (e.g. a stolen phone being drained).
        payee, category = _pick_payee(world, user, rng)
        times = _burst_times(now, rng.randint(5, 8), rng)
        return [_txn(user, payee, category, user.typical_amount * rng.uniform(0.8, 1.2),
                     device, user.city, ts, rng, **label) for ts in times]

    if pattern == "large_amount":
        payee, category = _pick_payee(world, user, rng)
        amount = user.typical_amount * rng.uniform(10, 30)
        return [_txn(user, payee, category, amount, device, user.city, now, rng, **label)]

    if pattern == "new_device_large":
        # Account takeover: a device we've never seen, often from another city, sending a lot.
        payee, category = _pick_payee(world, user, rng)
        amount = user.typical_amount * rng.uniform(5, 15)
        return [_txn(user, payee, category, amount, _new_device(rng), rng.choice(CITIES),
                     now, rng, **label)]

    if pattern == "many_payees":
        # Money mule fan-out: one payer sends to many different people quickly.
        others = [u for u in world.users if u.vpa != user.vpa]
        payees = rng.sample(others, rng.randint(8, 15))
        times = _burst_times(now, len(payees), rng)
        return [_txn(user, p.vpa, "p2p", user.typical_amount * rng.uniform(0.3, 1.0),
                     device, user.city, ts, rng, **label) for p, ts in zip(payees, times)]

    raise ValueError(f"unknown fraud pattern: {pattern}")


def make_dirty(dirty_type: str, txn: dict, rng: random.Random) -> list[dict]:
    """Corrupt a clean txn. Returns a list because 'duplicate' produces two records."""
    bad = dict(txn)  # copy so we never mutate the caller's record

    if dirty_type == "missing_field":
        del bad[rng.choice(REQUIRED_FIELDS)]
        return [bad]

    if dirty_type == "duplicate":
        # Same txn_id sent twice, like a producer retry or Kafka redelivery (at-least-once).
        return [bad, dict(bad)]

    if dirty_type == "malformed_vpa":
        which = rng.choice(["payer_vpa", "payee_vpa"])
        name, handle = bad[which].split("@")
        bad[which] = rng.choice([
            f"{name}{handle}",      # no @
            f"{name}@@{handle}",    # double @
            "",                     # empty
            f"@{handle}",           # no name part
            f"{name.upper()} X@{handle}",  # spaces / uppercase
        ])
        return [bad]

    if dirty_type == "negative_amount":
        bad["amount"] = -abs(bad["amount"])
        return [bad]

    raise ValueError(f"unknown dirty type: {dirty_type}")


def next_batch(world: World, rng: random.Random, now: datetime,
               fraud_rate: float, dirty_rate: float) -> tuple[str, list[dict]]:
    """Decide what kind of event happens next. Returns (kind, records).

    kind is only for the producer's log counters; it is NOT put into the records, so the
    processor has to detect dirty data by itself, just like in a real system.
    """
    r = rng.random()
    if r < fraud_rate:
        pattern = rng.choice(FRAUD_PATTERNS)
        return f"fraud:{pattern}", fraud_txns(pattern, world, rng, now)
    if r < fraud_rate + dirty_rate:
        dirty_type = rng.choice(DIRTY_TYPES)
        return f"dirty:{dirty_type}", make_dirty(dirty_type, normal_txn(world, rng, now), rng)
    return "normal", [normal_txn(world, rng, now)]
