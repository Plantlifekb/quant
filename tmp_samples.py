import os, traceback, psycopg2
tables = ["strategy_pnl_daily","strategy_positions_daily","returns_daily","prices_clean","market_regime_daily"]
try:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST","localhost"),
        port=os.getenv("POSTGRES_PORT","5432"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    cur = conn.cursor()
    for t in tables:
        try:
            cur.execute(f"SELECT * FROM {t} LIMIT 3;")
            rows = cur.fetchall()
            print(f"{t} sample rows:")
            for r in rows:
                print(r)
        except Exception as e:
            print(f"{t} error: {e}")
    cur.close()
    conn.close()
except Exception:
    traceback.print_exc()