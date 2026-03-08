import psycopg2
conn = psycopg2.connect(host="localhost", port=5432, dbname="quant", user="quant", password="quant")
with conn.cursor() as cur:
    cur.execute(
        "SELECT column_name, data_type, is_nullable "
        "FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name='prices' "
        "ORDER BY ordinal_position"
    )
    rows = cur.fetchall()
    for r in rows:
        print(r)
conn.close()
