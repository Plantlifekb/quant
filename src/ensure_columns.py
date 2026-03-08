import psycopg2
conn = psycopg2.connect(host="localhost", port=5432, dbname="quant", user="quant", password="quant")
with conn:
    with conn.cursor() as cur:
        cur.execute("""
ALTER TABLE public.prices
  ADD COLUMN IF NOT EXISTS adj_close numeric,
  ADD COLUMN IF NOT EXISTS high numeric,
  ADD COLUMN IF NOT EXISTS low numeric,
  ADD COLUMN IF NOT EXISTS open numeric,
  ADD COLUMN IF NOT EXISTS volume bigint;
""")
print("Columns ensured")
conn.close()
