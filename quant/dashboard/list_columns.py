import os
import psycopg2
from psycopg2.extras import RealDictCursor

tables = [
    'strategy_pnl_daily',
    'strategy_positions_daily',
    'returns_daily',
    'prices_clean',
    'market_regime_daily'
]

conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST', 'postgres'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)
cur = conn.cursor(cursor_factory=RealDictCursor)
for t in tables:
    try:
        cur.execute(
            '''
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
            ''',
            (t,)
        )
        cols = [r['column_name'] for r in cur.fetchall()]
        print(f"{t}:")
        for c in cols:
            print(f"  {c}")
    except Exception as e:
        print(f"{t} error: {e}")
cur.close()
conn.close()
