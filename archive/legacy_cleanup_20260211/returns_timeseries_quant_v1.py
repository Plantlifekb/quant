"""
Quant v1.1 — returns_timeseries_quant_v1.py
Converts wide panel returns into tidy (date, ticker, return) format.

Input:
- quant_returns_panel.csv

Output:
- quant_returns_timeseries_v1.csv
"""

import sys
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("returns_timeseries_quant_v1")

PANEL_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_returns_panel.csv"
OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_returns_timeseries_v1.csv"


def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def main():
    logger.info("Starting returns_timeseries_quant_v1 (v1.1).")

    df = pd.read_csv(PANEL_FILE)
    df.columns = [c.lower() for c in df.columns]

    if "date" not in df.columns:
        raise ValueError("quant_returns_panel.csv must contain a 'date' column.")

    df["date"] = pd.to_datetime(df["date"], utc=True)

    # Melt wide panel into long format
    long_df = df.melt(id_vars=["date"], var_name="ticker", value_name="return")

    long_df = long_df.sort_values(["date", "ticker"]).reset_index(drop=True)
    long_df["returns_run_date"] = iso_now()

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    long_df.to_csv(OUT_FILE, index=False, encoding="utf-8")

    logger.info("returns_timeseries_quant_v1 (v1.1) completed successfully.")


if __name__ == "__main__":
    main()