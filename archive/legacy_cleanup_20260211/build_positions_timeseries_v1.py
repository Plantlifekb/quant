"""
build_positions_timeseries_v1.py
Quant v1.0 — Governed Positions Pipeline
-----------------------------------------

Purpose:
    Construct the canonical positions timeseries dataset required by:
        - liquidity_costs_enhanced_v1
        - attribution_rolling_v1
        - attribution_sector_v1
        - attribution_regime_summary_v1
        - attribution_expected_vs_realised_v1

Inputs:
    quant_portfolio_weights_ensemble_risk_longshort_v2_trading.csv
    quant_market_liquidity_timeseries.csv
    (optional) quant_turnover_timeseries_ensemble_risk.csv

Output:
    quant_positions_timeseries.csv
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from logging_attribution_suite_v1 import get_logger

logger = get_logger("build_positions_timeseries_v1")

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data" / "analytics"

WEIGHTS_FILE = DATA_DIR / "quant_portfolio_weights_ensemble_risk_longshort_v2_trading.csv"
LIQ_FILE = DATA_DIR / "quant_market_liquidity_timeseries.csv"
TURNOVER_FILE = DATA_DIR / "quant_turnover_timeseries_ensemble_risk.csv"

OUT_FILE = DATA_DIR / "quant_positions_timeseries.csv"


def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------
def load_weights():
    logger.info("Loading canonical portfolio weights from %s", WEIGHTS_FILE)
    df = pd.read_csv(WEIGHTS_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "ticker", "weight_trading_v2"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in weights file: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.rename(columns={"weight_trading_v2": "weight"})

    return df


def load_liquidity():
    logger.info("Loading market liquidity (ADV, spreads) from %s", LIQ_FILE)
    df = pd.read_csv(LIQ_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "ticker", "adv_20", "spread_bps"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in liquidity file: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    return df


def load_turnover():
    if not TURNOVER_FILE.exists():
        logger.warning("Turnover file not found — turnover will be inferred.")
        return None

    logger.info("Loading turnover from %s", TURNOVER_FILE)
    df = pd.read_csv(TURNOVER_FILE)
    df.columns = [c.lower() for c in df.columns]

    if "turnover_raw" not in df.columns:
        logger.warning("turnover_raw not found — turnover will be inferred.")
        return None

    df["date"] = pd.to_datetime(df["date"], utc=True)
    return df[["date", "ticker", "turnover_raw"]]


# ---------------------------------------------------------------------
# Build positions dataset
# ---------------------------------------------------------------------
def build_positions():
    weights = load_weights()
    liq = load_liquidity()
    turnover = load_turnover()

    logger.info("Merging weights with liquidity data.")
    df = weights.merge(liq, on=["date", "ticker"], how="left")

    if turnover is not None:
        logger.info("Merging turnover data.")
        df = df.merge(turnover, on=["date", "ticker"], how="left")
    else:
        logger.info("Inferring turnover from weight changes.")
        df = df.sort_values(["ticker", "date"])
        df["weight_prev"] = df.groupby("ticker")["weight"].shift(1)
        df["turnover_raw"] = (df["weight"] - df["weight_prev"]).abs().fillna(0.0)

    df = df.fillna(0.0)

    logger.info("Positions dataset built with %d rows.", len(df))
    return df


# ---------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------
def save_positions(df):
    logger.info("Saving positions dataset to %s", OUT_FILE)
    df.to_csv(OUT_FILE, index=False, encoding="utf-8")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    logger.info("Starting build_positions_timeseries_v1.")
    df = build_positions()
    save_positions(df)
    logger.info("build_positions_timeseries_v1 completed successfully.")


if __name__ == "__main__":
    main()