import psycopg2
conn = psycopg2.connect(host="localhost", port=5432, dbname="quant", user="quant", password="quant")
with conn:
    with conn.cursor() as cur:
        cur.execute("""
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.prices'::regclass AND contype = 'p'
  ) THEN
    ALTER TABLE public.prices ADD PRIMARY KEY (date, ticker);
  END IF;
END
$$;
""")
print("Primary key ensured")
conn.close()
