"""
attribution_rolling_v1.py
Quant v1.0 — Attribution Suite
--------------------------------

Purpose:
    Produce rolling attribution metrics from ticker-level attribution.
    Computes rolling windows for:
        - factor contribution
        - idiosyncratic contribution
        - turnover cost
        - liquidity cost
        - net contribution

Inputs:
    C:/Quant/data/analytics/quant_attribution_regime_v1.csv

Output:
    C:/Quant/scripts/data/analytics/attribution_outputs_v1/attribution_rolling.csv
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from logging_attribution_suite_v1 import get_logger

logger = get_logger("attribution_rolling_v1")

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]

DATA_DIR = PROJECT_ROOT / "data" / "analytics"
ATTR_FILE = DATA_DIR / "quant_attribution_regime_v1.csv"

OUT_DIR = PROJECT_ROOT / "scripts" / "data" / "analytics" / "attribution_outputs_v1"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "attribution_rolling.csv"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------
# Load attribution
# ---------------------------------------------------------------------
def load_attribution() -> pd.DataFrame:
    logger.info("Loading ticker-level attribution from %s", ATTR_FILE)

    df = pd.read_csv(ATTR_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {
        "date",
        "regime_label",
        "factor_contribution",
        "idiosyncratic_contribution",
        "turnover_cost",
        "liquidity_cost",
        "net_contribution",
    }

    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values("date")

    numeric_cols = [
        "factor_contribution",
        "idiosyncratic_contribution",
        "turnover_cost",
        "liquidity_cost",
        "net_contribution",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------------------------------------------------------------------
# Compute rolling windows
# ---------------------------------------------------------------------
def compute_rolling(df: pd.DataFrame, window: int = 21) -> pd.DataFrame:
    logger.info("Computing %d-day rolling attribution.", window)

    numeric_cols = [
        "factor_contribution",
        "idiosyncratic_contribution",
        "turnover_cost",
        "liquidity_cost",
        "net_contribution",
    ]

    out = df.copy()
    out = out.set_index("date")

    for col in numeric_cols:
        out[f"{col}_rolling_{window}d"] = out[col].rolling(window).sum()

    out = out.reset_index()
    return out


# ---------------------------------------------------------------------
# Save output
# ---------------------------------------------------------------------
def save_output(df: pd.DataFrame, run_timestamp: str) -> None:
    logger.info("Saving rolling attribution to %s", OUT_FILE)

    out = df.copy()
    out["attribution_rolling_run_date"] = run_timestamp

    out.to_csv(OUT_FILE, index=False, encoding="utf-8")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    logger.info("Starting attribution_rolling_v1 run.")

    run_ts = iso_now()

    daily = load_attribution()
    rolling = compute_rolling(daily, window=21)
    save_output(rolling, run_ts)

    logger.info("attribution_rolling_v1 completed successfully.")


if __name__ == "__main__":
    main()