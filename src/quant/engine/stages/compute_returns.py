from quant.engine.db import get_engine


def compute_returns_stage():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute("SELECT 1")