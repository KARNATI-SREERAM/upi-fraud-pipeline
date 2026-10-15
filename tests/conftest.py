# Shared pytest fixtures: a real PostgreSQL connection that is rolled back after each test.
# Locally the DB tests are SKIPPED if Postgres isn't running; in CI (REQUIRE_DB=1) they FAIL
# instead, so a broken database setup can't hide behind "skipped".

import os

import psycopg
import pytest

DSN = os.getenv("TEST_DATABASE_URL", "postgresql://pipeline:pipeline@localhost:5433/upi")


def connect_or_skip() -> psycopg.Connection:
    try:
        return psycopg.connect(DSN, connect_timeout=2)
    except psycopg.OperationalError:
        if os.getenv("REQUIRE_DB") == "1":
            raise
        pytest.skip(f"PostgreSQL not reachable at {DSN} (run `docker compose up -d postgres`)")


@pytest.fixture
def db_conn():
    conn = connect_or_skip()
    yield conn
    conn.rollback()  # undo everything the test wrote
    conn.close()


@pytest.fixture
def cur(db_conn):
    with db_conn.cursor() as c:
        yield c
