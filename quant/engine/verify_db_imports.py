from quant.engine.db import get_engine


def verify():
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute("SELECT 1")