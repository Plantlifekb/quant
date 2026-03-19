import pandas as pd
import numpy as np
from sqlalchemy import text
from quant.engine.db import create_db_engine
from quant.logging_quant_v1 import log


def run():
    log.info("[returns] Starting returns computation...")

    engine = create_db_engine()

    # Load prices
    log.info("[returns] Loading prices...")
    prices = pd.read_sql(
        "SELECT date, ticker, close FROM prices ORDER BY date, ticker",
        engine
    )

    if prices.empty:
        log.error("[returns] No price data found.")
        return {"status": "no_price_data"}

    # Pivot to wide format
    log.info("[returns] Pivoting...")
    wide = prices.pivot(index="date", columns="ticker", values="close")

    # Compute simple daily returns
    log.info("[returns] Computing daily returns...")
    daily = wide.pct_change()

    # Drop all rows where ret is NaN (first date per ticker)
    daily = daily.dropna(how="all")

    # Melt back to long format
    log.info("[returns] Melting...")
    daily_long = daily.reset_index().melt(
        id_vars="date",
        var_name="ticker",
        value_name="ret"
    )

    # Drop any remaining NaN returns
    daily_long = daily_long.dropna(subset=["ret"])

    # Write to DB
    log.info("[returns] Writing to DB...")
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM returns"))
        daily_long.to_sql("returns", conn, if_exists="append", index=False)

    log.info("[returns] Completed.")
    return {"status": "ok", "rows": len(daily_long)}