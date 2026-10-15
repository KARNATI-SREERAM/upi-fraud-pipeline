# Validates and cleans one raw Kafka message. Pure function, no I/O, so every rule is unit-testable.
# Returns (clean_txn, None) if the record is usable, or (None, reason) if it must be quarantined.

import json
import math
import re
import uuid
from datetime import datetime, timedelta, timezone

# The processor keeps its own copy instead of importing producer code: in a real system the
# producer is another team's service, and this list is the contract we expect them to follow.
REQUIRED_FIELDS = ["txn_id", "payer_vpa", "payee_vpa", "amount", "timestamp", "device_id",
                   "location", "merchant_category"]
VPA_RE = re.compile(r"^[a-z0-9._-]+@[a-z]+$")
# Clocks on different machines drift. Allow a little, but reject events claiming to be far
# in the future.
MAX_CLOCK_SKEW = timedelta(minutes=5)


def validate(raw: bytes | None, now: datetime | None = None) -> tuple[dict | None, str | None]:
    now = now or datetime.now(timezone.utc)

    # 1. Parse
    if raw is None:
        return None, "invalid_json"
    try:
        rec = json.loads(raw)
    except ValueError:  # also covers bad UTF-8 (UnicodeDecodeError is a ValueError)
        return None, "invalid_json"
    if not isinstance(rec, dict):
        return None, "invalid_json"

    # 2. Required fields
    for f in REQUIRED_FIELDS:
        if rec.get(f) is None:
            return None, f"missing_field:{f}"

    # 3. txn_id must be a real UUID (it becomes the database primary key)
    try:
        txn_id = str(uuid.UUID(str(rec["txn_id"])))
    except ValueError:
        return None, "bad_txn_id"

    # 4. Clean: fix what is safe to fix. VPAs are case-insensitive, so
    # "Priya@OKSBI " == "priya@oksbi".
    payer = str(rec["payer_vpa"]).strip().lower()
    payee = str(rec["payee_vpa"]).strip().lower()

    # 5-6. VPA format, and no paying yourself
    if not VPA_RE.match(payer):
        return None, "malformed_vpa:payer"
    if not VPA_RE.match(payee):
        return None, "malformed_vpa:payee"
    if payer == payee:
        return None, "self_payment"

    # 7. Amount. bool is excluded because in Python True is an int (True == 1).
    # isfinite catches NaN/Infinity, which json.loads accepts and which pass a naive "> 0" check
    # (NaN > 0 and NaN <= 0 are both False).
    amount = rec["amount"]
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        return None, "bad_amount"
    if not math.isfinite(amount):
        return None, "bad_amount"
    if amount <= 0:
        return None, "non_positive_amount"

    # 8. Timestamp must be ISO-8601 with a timezone, and not in the future
    try:
        ts = datetime.fromisoformat(str(rec["timestamp"]))
    except ValueError:
        return None, "bad_timestamp"
    if ts.tzinfo is None:  # "12:00" means nothing without knowing which timezone
        return None, "bad_timestamp"
    if ts - now > MAX_CLOCK_SKEW:
        return None, "future_timestamp"

    return {
        "txn_id": txn_id,
        "payer_vpa": payer,
        "payee_vpa": payee,
        "amount": round(float(amount), 2),
        "timestamp": ts.isoformat(),
        "device_id": str(rec["device_id"]).strip(),
        "location": str(rec["location"]).strip(),
        "merchant_category": str(rec["merchant_category"]).strip().lower(),
        "is_fraud": rec.get("is_fraud"),
        "fraud_pattern": rec.get("fraud_pattern"),
    }, None
