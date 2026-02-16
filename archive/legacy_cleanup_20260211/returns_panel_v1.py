r"""
Quant v1.0 — returns_panel_v1.py
Version: v1.0

1. Module name
- returns_panel_v1

2. Quant version
- Quant v1.0

3. Purpose
- Build a canonical per-ticker daily returns panel from raw ingested prices.
- This panel is the standard input for:
  - portfolio_performance_v2
  - future attribution modules
  - risk and cost modelling

4. Inputs
- C:\Quant\data\ingestion\ingestion_5years.csv

  Required columns:
    - date
    - ticker
    - adj_close or close

  Logic:
    - If 'adj_close' exists, use it.
    - Else, if 'close' exists, use it.
    - Else, raise a governed error.

5. Outputs
- C:\Quant\data\analytics\quant_returns_panel.csv

  Columns:
    - date (ISO-8601)
    - ticker
    - daily_return

6. Governance rules
- No schema drift.
- All output columns lowercase.
- ISO-8601 dates only.
- Deterministic behaviour.

7. Dependencies
- pandas
- numpy
- logging_quant_v1

8. Provenance
- Governed component of Quant v1.0.
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("returns_panel_v1")

INGEST_FILE = PROJECT_ROOT / "data" / "ingestion" / "ingestion_5years.csv"
RET_PANEL_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_returns_panel.csv"


def load_ingested_prices() -> pd.DataFrame:
    logger.info("Loading ingested price history from %s", INGEST_FILE)

    df = pd.read_csv(INGEST_FILE)

    if "date" not in df.columns or "ticker" not in df.columns:
        msg = "ingestion_5years.csv must contain 'date' and 'ticker' columns."
        logger.error(msg)
        raise ValueError(msg)

    price_col = None
    if "adj_close" in df.columns:
        price_col = "adj_close"
    elif "close" in df.columns:
        price_col = "close"
    else:
        msg = "ingestion_5years.csv must contain 'adj_close' or 'close' column."
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", price_col])

    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df.dropna(subset=[price_col])

    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    logger.info(
        "Loaded %d rows from ingested prices using column '%s'.",
        len(df),
        price_col,
    )

    return df[["date", "ticker", price_col]].rename(columns={price_col: "price"})


def build_returns_panel(prices: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building daily returns panel from price history.")

    # Group by ticker and compute simple daily returns
    prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)

    def _compute_returns(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date")
        g["daily_return"] = g["price"].pct_change()
        return g

    df = prices.groupby("ticker", group_keys=False).apply(_compute_returns)
    df = df.dropna(subset=["daily_return"])

    df = df[["date", "ticker", "daily_return"]].copy()
    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)

    logger.info("Built %d daily return rows.", len(df))
    return df


def save_returns_panel(ret_panel: pd.DataFrame) -> None:
    logger.info("Saving returns panel to %s", RET_PANEL_FILE)
    ret_panel.to_csv(RET_PANEL_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting returns_panel_v1 run (v1.0).")

    prices = load_ingested_prices()
    ret_panel = build_returns_panel(prices)
    save_returns_panel(ret_panel)

    logger.info("returns_panel_v1 run completed successfully.")


if __name__ == "__main__":
    main()