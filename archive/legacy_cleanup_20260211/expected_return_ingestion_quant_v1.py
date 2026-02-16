r"""
Quant v1.0 — expected_return_ingestion_quant_v1.py
Version: v1.0

Purpose:
- Convert the composite alpha signal into the canonical expected-return file
  required by the OSQP Position Sizing Engine.

Input:
- quant_factors_composite.csv
    Required columns:
        date
        ticker
        composite_signal_v1

Output:
- quant_expected_returns_timeseries.csv
    Columns:
        date
        ticker
        expected_return
"""

import sys
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("expected_return_ingestion_quant_v1")

# ---------------------------------------------------------------------
# File locations
# ---------------------------------------------------------------------

SIGNAL_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_composite.csv"
OUT_EXPECTED_RETURN_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_expected_returns_timeseries.csv"

# ---------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------

def load_signals() -> pd.DataFrame:
    logger.info("Loading composite signal from %s", SIGNAL_FILE)
    df = pd.read_csv(SIGNAL_FILE)

    required = {"date", "ticker", "composite_signal_v1"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in quant_factors_composite.csv: {missing}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "composite_signal_v1"])

    df["expected_return"] = pd.to_numeric(df["composite_signal_v1"], errors="coerce")
    df = df.dropna(subset=["expected_return"])

    return df[["date", "ticker", "expected_return"]].sort_values(["date", "ticker"]).reset_index(drop=True)


def save_expected_returns(df: pd.DataFrame) -> None:
    logger.info("Saving expected returns to %s", OUT_EXPECTED_RETURN_FILE)
    df.to_csv(OUT_EXPECTED_RETURN_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting expected return ingestion (Quant v1.0).")

    df = load_signals()
    save_expected_returns(df)

    logger.info("Completed expected return ingestion successfully.")


if __name__ == "__main__":
    main()