# PostgreSQL writes for the processor: save a clean transaction or a quarantined record.
# Every write is idempotent (safe to repeat), which is what makes at-least-once delivery safe.

import psycopg


def connect(dsn: str) -> psycopg.Connection:
    # autocommit is off (the default): nothing is saved until main.py calls conn.commit(),
    # so a whole batch is saved together or not at all.
    return psycopg.connect(dsn)


def save_txn(cur: psycopg.Cursor, txn: dict, partition: int, offset: int) -> str:
    """Insert a clean txn. Returns "inserted", "redelivery" or "duplicate"."""
    cur.execute(
        """
        INSERT INTO transactions (txn_id, payer_vpa, payee_vpa, amount, ts, device_id, location,
                                  merchant_category, is_fraud, fraud_pattern,
                                  kafka_partition, kafka_offset)
        VALUES (%(txn_id)s, %(payer_vpa)s, %(payee_vpa)s, %(amount)s, %(timestamp)s, %(device_id)s,
                %(location)s, %(merchant_category)s, %(is_fraud)s, %(fraud_pattern)s,
                %(partition)s, %(offset)s)
        ON CONFLICT (txn_id) DO NOTHING
        """,
        {**txn, "partition": partition, "offset": offset},
    )
    if cur.rowcount == 1:
        return "inserted"

    # The txn_id already exists. Was it this exact Kafka message (redelivered after a crash),
    # or a different message carrying the same txn_id (a real duplicate)?
    cur.execute("SELECT kafka_partition, kafka_offset FROM transactions WHERE txn_id = %s",
                (txn["txn_id"],))
    if cur.fetchone() == (partition, offset):
        return "redelivery"
    return "duplicate"


def save_quarantine(cur: psycopg.Cursor, reason: str, raw_message: str,
                    partition: int, offset: int) -> str:
    """Store a rejected record. Returns "inserted", or "redelivery" if already quarantined."""
    cur.execute(
        """
        INSERT INTO quarantine (reason, raw_message, kafka_partition, kafka_offset)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
        """,
        (reason, raw_message, partition, offset),
    )
    return "inserted" if cur.rowcount == 1 else "redelivery"
