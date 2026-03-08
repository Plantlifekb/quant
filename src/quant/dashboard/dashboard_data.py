#!/usr/bin/env python3
# quant.dashboard.dashboard_data

import logging
import pandas as pd
from sqlalchemy import text

logger = logging.getLogger("dashboard_data")


def load_quant_dashboard_data(engine):
    """
    Load and prepare dashboard data from unified tables.

    This function is intentionally import‑safe:
    - It does NOT create a database engine.
    - It does NOT connect to Postgres at import time.
    - It only uses the engine passed in by the caller.

    The caller (dashboard app or orchestrator) is responsible for
    creating the engine lazily via create_db_engine().
    """

    logger.info("Loading dashboard datasets...")

    datasets = {}

    queries = {
        "strategy_performance": "SELECT * FROM strategy_performance ORDER BY date, ticker",
        "strategy_positions":   "SELECT * FROM strategy_positions ORDER BY date, ticker",
        "returns_daily":        "SELECT * FROM returns_daily ORDER BY date, ticker",
        "prices_clean":         "SELECT * FROM prices_clean ORDER BY date, ticker",
    }

    for name, sql in queries.items():
        try:
            datasets[name] = pd.read_sql(sql, engine)
        except Exception as e:
            logger.warning(f"{name} not available: {e}")

    logger.info("Dashboard datasets loaded.")
    return datasets