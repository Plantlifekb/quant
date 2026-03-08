from quant.engine.db import get_engine


def compute_factors():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute("SELECT 1")