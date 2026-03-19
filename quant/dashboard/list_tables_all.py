import os, psycopg2, traceback
try:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST","localhost"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog','information_schema')
        ORDER BY table_schema, table_name;
    """)
    for schema, name in cur.fetchall():
        print(f"{schema}.{name}")
    cur.close()
    conn.close()
except Exception:
    traceback.print_exc()
