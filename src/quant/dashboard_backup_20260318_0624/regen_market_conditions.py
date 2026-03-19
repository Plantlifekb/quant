#!/usr/bin/env python3
"""
regen_market_conditions.py — Compute market regimes, volatility regimes,
breadth, and VIX levels, and store them in the market_conditions table.

This script is idempotent and safe to re-run at any time.
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import logging

# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [market_conditions] %(levelname)s: %(message)s",
)

log = logging.getLogger("market_conditions")


# ---------------------------------------------------------
# Database connection
# ---------------------------------------------------------

DB_USER = os.environ.get("PGUSER", "quant")
DB_PASS = os.environ.get("PGPASSWORD", "quantpass")
DB_HOST = os.environ.get("PGHOST", "quant_postgres")
DB_NAME = os.environ.get("PGDATABASE", "quantdb")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}")


# ---------------------------------------------------------
# Load required data
# ---------------------------------------------------------

def load_returns() -> pd.DataFrame:
    query = """
        SELECT date, ticker, ret
        FROM returns_daily
        ORDER BY date, ticker
    """
    df = pd.read_sql(query, engine)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "ret"])

    return df


def load_vix() -> pd.DataFrame:
    query = """
        SELECT date, close AS vix
        FROM prices
        WHERE ticker = 'VIX'
        ORDER BY date
    """
    df = pd.read_sql(query, engine)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "vix"])

    return df


# ---------------------------------------------------------
# Compute market conditions
# ---------------------------------------------------------

def compute_market_conditions():
    log.info("Loading returns_daily...")
    df = load_returns()

    log.info("Loading VIX...")
    vix = load_vix()

    log.info("Computing market breadth...")
    breadth = (
        df.groupby("date")
        .apply(lambda x: (x["ret"] > 0).mean())
        .rename("breadth")
        .reset_index()
    )

    log.info("Computing volatility regime...")
    vol = (
        df.groupby("date")["ret"]
        .std()
        .rolling(21)
        .mean()
        .rename("vol_regime")
        .reset_index()
    )

    log.info("Computing market regime...")
    # Market regime based on SPY returns
    spy = df[df["ticker"] == "SPY"].copy()
    spy = spy.groupby("date")["ret"].mean().rolling(21).sum().rename("spy_21d").reset_index()

    def classify_regime(x):
        if x > 0.03:
            return "Bull"
        elif x < -0.03:
            return "Bear"
        else:
            return "Sideways"

    spy["regime"] = spy["spy_21d"].apply(classify_regime)

    log.info("Merging all components...")
    mc = (
        spy.merge(vol, on="date", how="left")
           .merge(breadth, on="date", how="left")
           .merge(vix, on="date", how="left")
    )

    mc = mc[["date", "regime", "vol_regime", "breadth", "vix"]]
    mc = mc.rename(columns={"vix": "vix_level"})

    mc = mc.dropna(subset=["date"])
    mc = mc.sort_values("date")

    log.info(f"Computed {len(mc)} rows of market conditions.")
    return mc


# ---------------------------------------------------------
# Write to database
# ---------------------------------------------------------

def ensure_table_exists():
    ddl = """
    CREATE TABLE IF NOT EXISTS market_conditions (
        date DATE PRIMARY KEY,
        regime TEXT,
        vol_regime DOUBLE PRECISION,
        breadth DOUBLE PRECISION,
        vix_level DOUBLE PRECISION
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    log.info("Ensured market_conditions table exists.")


def write_market_conditions(df: pd.DataFrame):
    log.info("Writing market conditions to database...")

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO market_conditions (date, regime, vol_regime, breadth, vix_level)
                    VALUES (:date, :regime, :vol_regime, :breadth, :vix_level)
                    ON CONFLICT (date) DO UPDATE SET
                        regime = EXCLUDED.regime,
                        vol_regime = EXCLUDED.vol_regime,
                        breadth = EXCLUDED.breadth,
                        vix_level = EXCLUDED.vix_level;
                """),
                {
                    "date": row["date"],
                    "regime": row["regime"],
                    "vol_regime": float(row["vol_regime"]) if not pd.isna(row["vol_regime"]) else None,
                    "breadth": float(row["breadth"]) if not pd.isna(row["breadth"]) else None,
                    "vix_level": float(row["vix_level"]) if not pd.isna(row["vix_level"]) else None,
                }
            )

    log.info("market_conditions table updated successfully.")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

if __name__ == "__main__":
    log.info("Starting market conditions regeneration...")

    ensure_table_exists()
    mc = compute_market_conditions()
    write_market_conditions(mc)

    log.info("Market conditions regeneration complete.")
