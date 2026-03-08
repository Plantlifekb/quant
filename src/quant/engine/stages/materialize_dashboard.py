from quant.engine.db import get_engine


def materialize_dashboard():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute("SELECT 1")