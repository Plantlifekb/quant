from .db import get_conn

def fetch(query, params=None):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        rows = cur.fetchall()
    conn.close()
    return rows