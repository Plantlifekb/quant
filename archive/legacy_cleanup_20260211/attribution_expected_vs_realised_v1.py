"""
liquidity_costs_enhanced_v1.py
Quant v1.0 — Attribution Suite
--------------------------------

Purpose:
    Compute enhanced liquidity cost metrics from positions timeseries,
    using ADV and spread information, and write a daily summary suitable
    for attribution dashboards.

Inputs:
    C:/Quant/data/analytics/quant_positions_timeseries.csv

Output:
    C:/Quant/scripts/data/analytics/attribution_outputs_v1/liquidity_costs_enhanced.csv
"""

from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from logging_attribution_suite_v1 import get_logger

logger = get_logger("liquidity_costs_enhanced_v1")

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]

DATA_DIR = PROJECT_ROOT / "data" / "analytics"

POS_FILE = DATA_DIR / "quant_positions_timeseries.csv"

OUT_DIR = PROJECT_ROOT / "scripts" / "data" / "analytics" / "attribution_outputs_v1"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "liquidity_costs_enhanced.csv"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------
def load_positions() -> pd.DataFrame:
    """
    Load positions timeseries.

    Expected minimal schema (all lowercase after load):
        - date
        - ticker
        - weight              (portfolio weight or position size)
        - adv_20              (20-day average daily volume, in same units as turnover)
        - spread_bps          (bid-ask spread in basis points)
        - turnover_raw        (optional; if absent, will be inferred from weight changes)
    """
    logger.info("Loading positions from %s", POS_FILE)

    df = pd.read_csv(POS_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "ticker", "weight", "adv_20", "spread_bps"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns in positions file: {sorted(missing)}"
        )

    df["date"] = pd.to_datetime(df["date"], utc=True)

    # Ensure numeric
    for col in ["weight", "adv_20", "spread_bps"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------
def compute_liquidity_costs(positions: pd.DataFrame) -> pd.DataFrame:
    """
    Compute enhanced liquidity cost metrics per (date, ticker) and aggregate daily.

    Heuristic:
        1. Infer turnover if not provided:
            turnover_raw = |weight_t - weight_{t-1}| per ticker
        2. Compute participation rate:
            participation = min(1, turnover_raw / adv_20)
        3. Compute liquidity cost in bps:
            liquidity_cost_bps = spread_bps * participation
        4. Convert to contribution units:
            liquidity_cost = liquidity_cost_bps / 10_000 * |weight|
        5. Aggregate to daily totals.
    """
    df = positions.copy()

    # Infer turnover_raw if not present
    if "turnover_raw" not in df.columns:
        logger.info("turnover_raw not found; inferring from weight changes.")
        df = df.sort_values(["ticker", "date"])
        df["weight_prev"] = df.groupby("ticker")["weight"].shift(1)
        df["turnover_raw"] = (df["weight"] - df["weight_prev"]).abs()
        df["turnover_raw"] = df["turnover_raw"].fillna(0.0)
    else:
        df["turnover_raw"] = pd.to_numeric(df["turnover_raw"], errors="coerce").fillna(0.0)

    # Avoid division by zero
    df["adv_20"] = df["adv_20"].replace(0, np.nan)

    # Participation rate capped at 1
    df["participation"] = (df["turnover_raw"] / df["adv_20"]).clip(lower=0.0, upper=1.0)
    df["participation"] = df["participation"].fillna(0.0)

    # Liquidity cost in bps and in contribution units
    df["liquidity_cost_bps_enhanced"] = df["spread_bps"] * df["participation"]
    df["liquidity_cost_enhanced"] = (
        df["liquidity_cost_bps_enhanced"] / 10_000.0 * df["weight"].abs()
    )

    # Daily aggregates
    daily = (
        df.groupby("date")
        .agg(
            total_liquidity_cost_enhanced=("liquidity_cost_enhanced", "sum"),
            avg_liquidity_cost_bps_enhanced=("liquidity_cost_bps_enhanced", "mean"),
            avg_participation=("participation", "mean"),
        )
        .reset_index()
    )

    return df, daily


# ---------------------------------------------------------------------
# Save output
# ---------------------------------------------------------------------
def save_output(
    per_ticker: pd.DataFrame,
    daily: pd.DataFrame,
    run_timestamp: str,
) -> None:
    """
    Save enhanced liquidity costs.

    We write a single CSV with both per-ticker and daily aggregates,
    distinguished by a 'level' column.
    """
    logger.info("Saving enhanced liquidity costs to %s", OUT_FILE)

    per_ticker_out = per_ticker.copy()
    per_ticker_out["level"] = "ticker"
    per_ticker_out["liquidity_costs_enhanced_run_date"] = run_timestamp

    daily_out = daily.copy()
    daily_out["level"] = "daily"
    daily_out["liquidity_costs_enhanced_run_date"] = run_timestamp

    # Align columns
    common_cols = sorted(
        set(per_ticker_out.columns) | set(daily_out.columns)
    )
    per_ticker_out = per_ticker_out.reindex(columns=common_cols)
    daily_out = daily_out.reindex(columns=common_cols)

    out = pd.concat([per_ticker_out, daily_out], ignore_index=True)
    out.to_csv(OUT_FILE, index=False, encoding="utf-8")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    logger.info("Starting liquidity_costs_enhanced_v1 run.")

    run_ts = iso_now()

    positions = load_positions()
    per_ticker, daily = compute_liquidity_costs(positions)
    save_output(per_ticker, daily, run_ts)

    logger.info("liquidity_costs_enhanced_v1 completed successfully.")


if __name__ == "__main__":
    main()