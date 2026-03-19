# quant/dashboard/data_access.py
import os
import psycopg2
import pandas as pd

def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "quant"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quant"),
    )

def load_strategies():
    query = """
        SELECT
            id,
            name,
            COALESCE(label, name) AS label
        FROM strategies
        ORDER BY name;
    """
    with get_db_conn() as conn:
        return pd.read_sql(query, conn)

def load_strategy_pnl_daily(strategy_id: int):
    query = """
        SELECT
            date,
            strategy_id,
            pnl,
            cumulative_pnl,
            return
        FROM strategy_pnl_daily
        WHERE strategy_id = %s
        ORDER BY date;
    """
    with get_db_conn() as conn:
        return pd.read_sql(query, conn, params=(strategy_id,))

def load_market_regime():
    query = """
        SELECT
            date,
            regime
        FROM market_regime_daily
        ORDER BY date;
    """
    with get_db_conn() as conn:
        return pd.read_sql(query, conn)

def load_market_events():
    query = """
        SELECT
            e.id,
            e.event_type,
            e.label,
            l.event_time::date AS date
        FROM market_events e
        JOIN event_log l
          ON l.event_id = e.id
        ORDER BY l.event_time;
    """
    with get_db_conn() as conn:
        return pd.read_sql(query, conn)