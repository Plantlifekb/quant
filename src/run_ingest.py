# run_ingest.py
import os
import glob
import psycopg2

# Adjust these env vars if needed; scheduler will set them too
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "quant_test_db")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "postgres")

def apply_sql_migrations(migrations_dir="migrations"):
    sql_files = sorted(glob.glob(os.path.join(migrations_dir, "*.sql")))
    if not sql_files:
        print("No migrations found")
        return
    conn = psycopg2.connect(host=PGHOST, port=PGPORT, dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD)
    try:
        with conn:
            with conn.cursor() as cur:
                for path in sql_files:
                    print("Applying", path)
                    with open(path, "r", encoding="utf8") as fh:
                        cur.execute(fh.read())
        print("Migrations applied")
    finally:
        conn.close()

def run_ingest_task():
    # Import the task and run it
    from quant.engine.tasks.ingestion import task_ingest_and_write
    result = task_ingest_and_write()
    print("Ingest result:", result)

if __name__ == "__main__":
    apply_sql_migrations()
    run_ingest_task()