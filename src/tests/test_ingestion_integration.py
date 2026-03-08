# tests/test_ingestion_integration.py
import os
import time
import json
import subprocess
import psycopg2
import pytest

# Configuration for the test Postgres container
PG_USER = "quant"
PG_PASS = "quant"
PG_DB = "quant"
PG_PORT = "5433"  # use non-default to avoid collisions
PG_CONTAINER = "quant_test_db"

@pytest.fixture(scope="session")
def postgres_container():
    # Start a temporary Postgres container
    subprocess.run([
        "docker", "run", "--rm", "--name", PG_CONTAINER,
        "-e", f"POSTGRES_USER={PG_USER}",
        "-e", f"POSTGRES_PASSWORD={PG_PASS}",
        "-e", f"POSTGRES_DB={PG_DB}",
        "-p", f"{PG_PORT}:5432",
        "-d", "postgres:15"
    ], check=True)

    # Wait for Postgres to become ready
    for _ in range(30):
        try:
            conn = psycopg2.connect(host="localhost", port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)
            conn.close()
            break
        except Exception:
            time.sleep(1)
    else:
        # If not ready after retries, stop container and fail
        subprocess.run(["docker", "logs", PG_CONTAINER], check=False)
        subprocess.run(["docker", "rm", "-f", PG_CONTAINER], check=False)
        pytest.fail("Postgres container failed to start")

    yield

    # Teardown: stop container
    subprocess.run(["docker", "rm", "-f", PG_CONTAINER], check=False)

@pytest.mark.usefixtures("postgres_container")
def test_task_ingest_and_write_integration(tmp_path, monkeypatch):
    # Point the ingestion code at the test Postgres
    os.environ["PGHOST"] = "localhost"
    os.environ["PGPORT"] = PG_PORT
    os.environ["PGDATABASE"] = PG_DB
    os.environ["PGUSER"] = PG_USER
    os.environ["PGPASSWORD"] = PG_PASS

    # Import the ingestion function under test
    from quant.engine.tasks.ingestion import task_ingest_and_write

    # Run ingestion
    result = task_ingest_and_write()

    # Basic assertions on wrapper output
    assert isinstance(result, dict)
    assert result.get("status") == "ok"
    rows_written = result.get("rows_written")
    assert isinstance(rows_written, int) and rows_written > 0
    last_date = result.get("last_date")
    assert last_date is not None

    # Verify DB contents: connect and check MAX(date) and row count
    dsn = dict(host="localhost", port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    with psycopg2.connect(**dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.prices;")
            count = cur.fetchone()[0]
            assert count >= rows_written or count == rows_written

            cur.execute("SELECT TO_CHAR(MAX(date),'YYYY-MM-DD') FROM public.prices;")
            db_max = cur.fetchone()[0]
            assert db_max == last_date