r"""
Quant v1.0 — vol_target_quant_v1.py
Version: v1.0

1. Module name
- vol_target_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Apply volatility targeting to existing portfolio performance:
  - Input: quant_portfolio_performance.csv
  - Output: quant_portfolio_performance_voltarget.csv
- For each portfolio_type (longonly, longshort):
  - Estimate rolling realized volatility
  - Compute a daily scaling factor to hit a target annualized volatility
  - Generate volatility-targeted daily and cumulative returns

4. Inputs
- C:\Quant\data\analytics\quant_portfolio_performance.csv

  Required columns:
    - date
    - portfolio_type
    - daily_return
    - cumulative_return

5. Outputs
- C:\Quant\data\analytics\quant_portfolio_performance_voltarget.csv

  Columns:
    - date
    - portfolio_type
    - daily_return
    - cumulative_return
    - vt_scaling_factor
    - vt_daily_return
    - vt_cumulative_return

6. Governance rules
- No schema drift.
- No silent changes.
- All output columns lowercase.
- ISO-8601 dates only.
- Deterministic, reproducible behaviour.
- No writing outside governed directories.

7. Logging rules
- Uses logging_quant_v1.py
- Logs start, end, and key events.
- Logs errors narratably.

8. Encoding rules
- All CSV outputs UTF-8.

9. Dependencies
- pandas
- numpy
- logging_quant_v1

10. Provenance
- This module is a governed component of Quant v1.0.
- Any modification requires version bump and architecture update.
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd

# Project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("vol_target_quant_v1")

# Files
PERF_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance.csv"
PERF_VT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance_voltarget.csv"

# Vol targeting parameters
TARGET_ANNUAL_VOL = 0.20      # 20% annualized target volatility
ROLLING_WINDOW_DAYS = 60      # lookback window for realized vol
TRADING_DAYS_PER_YEAR = 252   # used to annualize volatility
MAX_SCALING = 3.0             # cap scaling factor to avoid extreme leverage
MIN_SCALING = 0.0             # floor at 0


def load_performance() -> pd.DataFrame:
    logger.info(f"Loading portfolio performance from {PERF_FILE}")
    df = pd.read_csv(PERF_FILE)

    required = {"date", "portfolio_type", "daily_return", "cumulative_return"}
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing required columns in performance file: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "portfolio_type", "daily_return"])

    df["daily_return"] = pd.to_numeric(df["daily_return"], errors="coerce")
    df = df.dropna(subset=["daily_return"])

    logger.info(f"Loaded {len(df)} rows after cleaning.")
    return df


def apply_vol_targeting(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(
        f"Applying volatility targeting: target_annual_vol={TARGET_ANNUAL_VOL}, "
        f"window={ROLLING_WINDOW_DAYS} days."
    )

    def _per_portfolio(group: pd.DataFrame) -> pd.DataFrame:
        g = group.sort_values("date").copy()

        # Rolling realized daily volatility
        rolling_std = (
            g["daily_return"]
            .rolling(window=ROLLING_WINDOW_DAYS, min_periods=ROLLING_WINDOW_DAYS)
            .std()
        )

        # Annualize
        realized_annual_vol = rolling_std * np.sqrt(TRADING_DAYS_PER_YEAR)

        # Scaling factor
        scaling = TARGET_ANNUAL_VOL / realized_annual_vol
        scaling = scaling.clip(lower=MIN_SCALING, upper=MAX_SCALING)

        # Before we have enough history, use scaling = 1.0 (no targeting)
        scaling = scaling.fillna(1.0)

        g["vt_scaling_factor"] = scaling
        g["vt_daily_return"] = g["daily_return"] * g["vt_scaling_factor"]

        # Vol-targeted cumulative return as running sum of vt_daily_return
        g["vt_cumulative_return"] = g["vt_daily_return"].cumsum()

        return g

    out = (
        df
        .groupby("portfolio_type", group_keys=False)
        .apply(_per_portfolio)
        .reset_index(drop=True)
    )

    logger.info("Volatility targeting applied to all portfolio types.")
    return out


def save_output(df: pd.DataFrame) -> None:
    logger.info(f"Saving volatility-targeted performance to {PERF_VT_FILE}")
    df.to_csv(PERF_VT_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting vol_target_quant_v1 run (v1.0).")

    perf = load_performance()
    perf_vt = apply_vol_targeting(perf)
    save_output(perf_vt)

    logger.info("vol_target_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()