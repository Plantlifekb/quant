"""
attribution_regime_summary_v1.py
Quant v1.0 — Attribution Suite
--------------------------------

Purpose:
    Build a daily regime-level attribution summary from ticker-level attribution.
    Aggregates:
        - factor contribution
        - idiosyncratic contribution
        - turnover cost
        - liquidity cost
        - net contribution

Inputs:
    C:/Quant/data/analytics/quant_attribution_regime_v1.csv

Output:
    C:/Quant/scripts/data/analytics/attribution_outputs_v1/attribution_regime_summary.csv
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from logging_attribution_suite_v1 import get_logger

logger = get_logger("attribution_regime_summary_v1")

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]

DATA_DIR = PROJECT_ROOT / "data" / "analytics"
ATTR_FILE = DATA_DIR / "quant_attribution_regime_v1.csv"

OUT_DIR = PROJECT_ROOT / "scripts" / "data" / "analytics" / "attribution_outputs_v1"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "attribution_regime_summary.csv"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------
# Load ticker-level attribution
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

    # Coerce numeric columns
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
# Build regime-level summary
# ---------------------------------------------------------------------
def build_regime_summary(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Aggregating attribution by date and regime_label.")

    numeric_cols = [
        "factor_contribution",
        "idiosyncratic_contribution",
        "turnover_cost",
        "liquidity_cost",
        "net_contribution",
    ]

    summary = (
        df.groupby(["date", "regime_label"])[numeric_cols]
        .sum()
        .reset_index()
        .sort_values(["date", "regime_label"])
    )

    return summary


# ---------------------------------------------------------------------
# Save output
# ---------------------------------------------------------------------
def save_output(df: pd.DataFrame, run_timestamp: str) -> None:
    logger.info("Saving regime-level attribution summary to %s", OUT_FILE)

    out = df.copy()
    out["attribution_regime_summary_run_date"] = run_timestamp

    out.to_csv(OUT_FILE, index=False, encoding="utf-8")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    logger.info("Starting attribution_regime_summary_v1 run.")

    run_ts = iso_now()

    df = load_attribution()
    summary = build_regime_summary(df)
    save_output(summary, run_ts)

    logger.info("attribution_regime_summary_v1 completed successfully.")


if __name__ == "__main__":
    main()