"""
factor_returns_alignment_v1.py
Quant v1.0 — Attribution Suite
--------------------------------

Purpose:
    Align raw factor returns with the trading calendar used by the attribution suite.
    - Enforces a clean, continuous date index
    - Handles duplicate dates safely (mean aggregation)
    - Coerces non-numeric values to NaN with logging
    - Ensures all factor columns are lowercased
    - Writes a governed, aligned factor-returns file for downstream modules

Inputs:
    C:/Quant/data/analytics/quant_factor_returns_regime_v1.csv

Output:
    C:/Quant/scripts/data/analytics/attribution_outputs_v1/factor_returns_aligned.csv
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from logging_attribution_suite_v1 import get_logger

logger = get_logger("factor_returns_alignment_v1")

# ---------------------------------------------------------------------
# Paths (governed, aligned with your current layout)
# ---------------------------------------------------------------------
# Quant root is C:/Quant
PROJECT_ROOT = Path(__file__).resolve().parents[3]

DATA_DIR = PROJECT_ROOT / "data" / "analytics"
RAW_FACTOR_RETURNS_FILE = DATA_DIR / "quant_factor_returns_regime_v1.csv"

# Outputs live under scripts/data/analytics/attribution_outputs_v1
OUT_DIR = PROJECT_ROOT / "scripts" / "data" / "analytics" / "attribution_outputs_v1"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "factor_returns_aligned.csv"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------
# Load raw factor returns
# ---------------------------------------------------------------------
def load_raw_factor_returns() -> pd.DataFrame:
    logger.info("Loading raw factor returns from %s", RAW_FACTOR_RETURNS_FILE)

    df = pd.read_csv(RAW_FACTOR_RETURNS_FILE)
    df.columns = [c.lower() for c in df.columns]

    if "date" not in df.columns:
        raise ValueError("Input factor returns file must contain a 'date' column.")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values("date")

    return df


# ---------------------------------------------------------------------
# Build trading calendar
# ---------------------------------------------------------------------
def build_trading_calendar(df: pd.DataFrame) -> pd.DatetimeIndex:
    logger.info("Building trading calendar from factor returns date range.")

    start = df["date"].min()
    end = df["date"].max()

    calendar = pd.date_range(start=start, end=end, freq="D", tz="UTC")

    logger.info("Trading calendar from %s to %s (%d days).", start, end, len(calendar))
    return calendar


# ---------------------------------------------------------------------
# Align factor returns to calendar
# ---------------------------------------------------------------------
def align_factor_returns(df: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.DataFrame:
    logger.info("Aligning factor returns to trading calendar.")

    df = df.set_index("date").sort_index()

    # -------------------------------------------------------------
    # Handle duplicate dates safely
    # -------------------------------------------------------------
    if df.index.has_duplicates:
        dupes = df.index[df.index.duplicated()].unique()
        logger.warning("Duplicate factor-return dates detected: %s", list(dupes))

        # Convert all columns to numeric BEFORE aggregation
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Log columns where coercion introduced NaNs
        bad_cols = [col for col in df.columns if df[col].isna().any()]
        if bad_cols:
            logger.warning(
                "Non-numeric values detected in columns %s; coerced to NaN before aggregation.",
                bad_cols,
            )

        # Aggregate duplicates by mean
        df = df.groupby(df.index).mean()

    # -------------------------------------------------------------
    # Now safe to reindex
    # -------------------------------------------------------------
    aligned = df.reindex(calendar)

    aligned.index.name = "date"
    aligned = aligned.reset_index()

    # Ensure all non-date columns are numeric
    for col in aligned.columns:
        if col == "date":
            continue
        aligned[col] = pd.to_numeric(col, errors="coerce")

    return aligned


# ---------------------------------------------------------------------
# Save output
# ---------------------------------------------------------------------
def save_output(df: pd.DataFrame, run_timestamp: str) -> None:
    logger.info("Saving aligned factor returns to %s", OUT_FILE)

    out = df.copy()
    out["factor_returns_alignment_run_date"] = run_timestamp

    out.to_csv(OUT_FILE, index=False, encoding="utf-8")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    logger.info("Starting factor_returns_alignment_v1 run.")

    run_ts = iso_now()

    raw = load_raw_factor_returns()
    calendar = build_trading_calendar(raw)
    aligned = align_factor_returns(raw, calendar)
    save_output(aligned, run_ts)

    logger.info("factor_returns_alignment_v1 completed successfully.")


if __name__ == "__main__":
    main()