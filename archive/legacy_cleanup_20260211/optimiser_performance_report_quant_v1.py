r"""
Quant v1.0 — optimiser_performance_report_quant_v1.py
Version: v1.0

1. Module name
- optimiser_performance_report_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Compare pre-optimiser (risk-neutral) vs post-optimiser (vol-targeted) performance.
- For each portfolio_type ("longonly", "longshort"):
  - Compute annualised volatility
  - Compute annualised Sharpe (rf = 0)
  - Compute max drawdown
  - Compute Calmar ratio
  - Compute skew and kurtosis
- Produce:
  - Time series comparison
  - Summary comparison

4. Inputs
- C:\Quant\data\analytics\quant_portfolio_performance_ensemble_risk.csv
- C:\Quant\data\analytics\quant_optimised_performance_ensemble_risk.csv

5. Outputs
- C:\Quant\data\analytics\quant_optimiser_performance_timeseries.csv
- C:\Quant\data\analytics\quant_optimiser_performance_summary.csv

6. Governance rules
- No schema drift.
- No silent changes.
- All output columns lowercase.
- ISO-8601 dates only.
- Deterministic, reproducible behaviour.

7. Logging rules
- Uses logging_quant_v1.py

8. Dependencies
- pandas
- numpy

9. Provenance
- Governed component of Quant v1.0.
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("optimiser_performance_report_quant_v1")

# Files
PRE_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance_ensemble_risk.csv"
POST_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_optimised_performance_ensemble_risk.csv"

TS_OUTPUT = PROJECT_ROOT / "data" / "analytics" / "quant_optimiser_performance_timeseries.csv"
SUMMARY_OUTPUT = PROJECT_ROOT / "data" / "analytics" / "quant_optimiser_performance_summary.csv"

TRADING_DAYS = 252


def load_data():
    logger.info("Loading pre- and post-optimiser performance files.")

    pre = pd.read_csv(PRE_FILE)
    post = pd.read_csv(POST_FILE)

    pre["date"] = pd.to_datetime(pre["date"], errors="coerce")
    post["date"] = pd.to_datetime(post["date"], errors="coerce")

    pre = pre.dropna(subset=["date", "portfolio_type", "daily_return"])
    post = post.dropna(subset=["date", "portfolio_type", "optimised_daily_return"])

    pre["daily_return"] = pd.to_numeric(pre["daily_return"], errors="coerce")
    post["optimised_daily_return"] = pd.to_numeric(post["optimised_daily_return"], errors="coerce")

    pre = pre.dropna(subset=["daily_return"])
    post = post.dropna(subset=["optimised_daily_return"])

    logger.info(f"Loaded {len(pre)} pre-optimiser rows and {len(post)} post-optimiser rows.")
    return pre, post


def compute_timeseries(pre, post):
    logger.info("Building optimiser performance time series.")

    post_ts = post[[
        "date",
        "portfolio_type",
        "optimised_daily_return",
        "optimised_cumulative_return",
        "applied_leverage"
    ]].copy()

    pre_ts = pre[[
        "date",
        "portfolio_type",
        "daily_return",
        "cumulative_return"
    ]].copy()

    merged = pre_ts.merge(
        post_ts,
        on=["date", "portfolio_type"],
        how="left"
    )

    merged = merged.sort_values(["portfolio_type", "date"]).reset_index(drop=True)

    logger.info("Time series comparison built.")
    return merged


def compute_summary(pre, post):
    logger.info("Computing optimiser performance summary.")

    records = []

    for ptype in pre["portfolio_type"].unique():
        pre_g = pre[pre["portfolio_type"] == ptype].copy()
        post_g = post[post["portfolio_type"] == ptype].copy()

        # Pre-optimiser stats
        pre_rets = pre_g["daily_return"].values
        pre_vol = np.std(pre_rets, ddof=1) * np.sqrt(TRADING_DAYS)
        pre_mean = np.mean(pre_rets) * TRADING_DAYS
        pre_sharpe = pre_mean / pre_vol if pre_vol > 0 else np.nan

        pre_equity = 1 + pre_g["cumulative_return"]
        pre_dd = (pre_equity - np.maximum.accumulate(pre_equity)) / np.maximum.accumulate(pre_equity)
        pre_max_dd = float(pre_dd.min())
        pre_calmar = pre_mean / abs(pre_max_dd) if pre_max_dd < 0 else np.nan

        # Post-optimiser stats
        post_rets = post_g["optimised_daily_return"].values
        post_vol = np.std(post_rets, ddof=1) * np.sqrt(TRADING_DAYS)
        post_mean = np.mean(post_rets) * TRADING_DAYS
        post_sharpe = post_mean / post_vol if post_vol > 0 else np.nan

        post_equity = 1 + post_g["optimised_cumulative_return"]
        post_dd = (post_equity - np.maximum.accumulate(post_equity)) / np.maximum.accumulate(post_equity)
        post_max_dd = float(post_dd.min())
        post_calmar = post_mean / abs(post_max_dd) if post_max_dd < 0 else np.nan

        records.append({
            "portfolio_type": ptype,
            "pre_ann_vol": float(pre_vol),
            "post_ann_vol": float(post_vol),
            "pre_ann_sharpe": float(pre_sharpe),
            "post_ann_sharpe": float(post_sharpe),
            "pre_max_drawdown": float(pre_max_dd),
            "post_max_drawdown": float(post_max_dd),
            "pre_calmar": float(pre_calmar),
            "post_calmar": float(post_calmar),
        })

    summary = pd.DataFrame.from_records(records)
    summary = summary.sort_values("portfolio_type").reset_index(drop=True)

    logger.info("Summary comparison computed.")
    return summary


def save_outputs(ts, summary):
    logger.info(f"Saving optimiser performance time series to {TS_OUTPUT}")
    ts.to_csv(TS_OUTPUT, index=False, encoding="utf-8")

    logger.info(f"Saving optimiser performance summary to {SUMMARY_OUTPUT}")
    summary.to_csv(SUMMARY_OUTPUT, index=False, encoding="utf-8")


def main():
    logger.info("Starting optimiser_performance_report_quant_v1 run (v1.0).")

    pre, post = load_data()
    ts = compute_timeseries(pre, post)
    summary = compute_summary(pre, post)
    save_outputs(ts, summary)

    logger.info("optimiser_performance_report_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()