import os, psycopg2
conn = psycopg2.connect(host=os.environ['PGHOST'], port=os.environ['PGPORT'],
                        dbname=os.environ['PGDATABASE'], user=os.environ['PGUSER'],
                        password=os.environ['PGPASSWORD'])
with conn:
    with conn.cursor() as cur:
        cur.execute('TRUNCATE TABLE public.prices;')
print('TRUNCATE OK')
conn.close()
