r"""
Quant v1.0 — risk_report_quant_v1.py
Version: v1.0

1. Module name
- risk_report_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Compute governed risk diagnostics for risk-neutral ensemble portfolios:
  - longshort
  - longonly
- For each portfolio_type:
  - Daily risk metrics:
    - rolling volatility
    - rolling Sharpe (rf = 0)
    - cumulative return
    - drawdown
  - Summary risk metrics:
    - annualised volatility
    - annualised Sharpe
    - max drawdown
    - Calmar ratio
    - skewness
    - kurtosis

4. Inputs
- C:\Quant\data\analytics\quant_portfolio_performance_ensemble_risk.csv

  Required columns:
    - date
    - portfolio_type   ("longshort" / "longonly")
    - daily_return
    - cumulative_return

5. Outputs
- C:\Quant\data\analytics\quant_risk_timeseries_ensemble_risk.csv
- C:\Quant\data\analytics\quant_risk_summary_ensemble_risk.csv

  risk_timeseries columns:
    - date
    - portfolio_type
    - daily_return
    - cumulative_return
    - rolling_vol_63
    - rolling_sharpe_63
    - drawdown

  risk_summary columns:
    - portfolio_type
    - ann_vol
    - ann_sharpe
    - max_drawdown
    - calmar
    - skew
    - kurtosis

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

logger = get_logger("risk_report_quant_v1")

# Files
PERF_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance_ensemble_risk.csv"

TS_OUTPUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_risk_timeseries_ensemble_risk.csv"
SUMMARY_OUTPUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_risk_summary_ensemble_risk.csv"

# Assumptions
TRADING_DAYS_PER_YEAR = 252
ROLLING_WINDOW = 63  # ~3 months


def load_performance() -> pd.DataFrame:
    logger.info(f"Loading risk-neutral portfolio performance from {PERF_FILE}")
    df = pd.read_csv(PERF_FILE)

    required = {"date", "portfolio_type", "daily_return", "cumulative_return"}
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing required columns in performance file: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "portfolio_type", "daily_return", "cumulative_return"])

    df["daily_return"] = pd.to_numeric(df["daily_return"], errors="coerce")
    df["cumulative_return"] = pd.to_numeric(df["cumulative_return"], errors="coerce")
    df = df.dropna(subset=["daily_return", "cumulative_return"])

    df = df.sort_values(["portfolio_type", "date"]).reset_index(drop=True)

    logger.info(f"Loaded {len(df)} performance rows after cleaning.")
    return df[["date", "portfolio_type", "daily_return", "cumulative_return"]]


def _rolling_stats(series: pd.Series, window: int) -> tuple[pd.Series, pd.Series]:
    """
    Compute rolling volatility and rolling Sharpe (rf=0) over a given window.
    """
    roll_vol = series.rolling(window=window, min_periods=1).std()
    roll_sharpe = (series.rolling(window=window, min_periods=1).mean() /
                   roll_vol.replace(0, np.nan))
    return roll_vol, roll_sharpe


def compute_timeseries_risk(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing time series risk metrics for risk-neutral portfolios.")

    records = []

    for ptype, group in df.groupby("portfolio_type"):
        g = group.sort_values("date").reset_index(drop=True).copy()

        # Rolling stats on daily returns
        roll_vol, roll_sharpe = _rolling_stats(g["daily_return"], ROLLING_WINDOW)

        # Drawdown from cumulative return
        # cumulative_return is additive; convert to equity curve: equity = 1 + cumulative_return
        equity = 1.0 + g["cumulative_return"]
        running_max = equity.cummax()
        drawdown = (equity - running_max) / running_max

        for i in range(len(g)):
            records.append(
                {
                    "date": g.loc[i, "date"],
                    "portfolio_type": ptype,
                    "daily_return": float(g.loc[i, "daily_return"]),
                    "cumulative_return": float(g.loc[i, "cumulative_return"]),
                    "rolling_vol_63": float(roll_vol.iloc[i]) if not pd.isna(roll_vol.iloc[i]) else np.nan,
                    "rolling_sharpe_63": float(roll_sharpe.iloc[i]) if not pd.isna(roll_sharpe.iloc[i]) else np.nan,
                    "drawdown": float(drawdown.iloc[i]),
                }
            )

    ts = pd.DataFrame.from_records(records)
    ts = ts.sort_values(["portfolio_type", "date"]).reset_index(drop=True)

    logger.info("Time series risk metrics computed.")
    return ts


def compute_summary_risk(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing summary risk metrics for risk-neutral portfolios.")

    records = []

    for ptype, group in df.groupby("portfolio_type"):
        g = group.sort_values("date").reset_index(drop=True).copy()
        rets = g["daily_return"].values

        if len(rets) == 0:
            continue

        # Annualised volatility
        daily_vol = np.std(rets, ddof=1)
        ann_vol = daily_vol * np.sqrt(TRADING_DAYS_PER_YEAR)

        # Annualised Sharpe (rf = 0)
        daily_mean = np.mean(rets)
        ann_mean = daily_mean * TRADING_DAYS_PER_YEAR
        ann_sharpe = ann_mean / ann_vol if ann_vol > 0 else np.nan

        # Max drawdown (using cumulative_return)
        equity = 1.0 + g["cumulative_return"]
        running_max = np.maximum.accumulate(equity)
        drawdowns = (equity - running_max) / running_max
        max_drawdown = float(drawdowns.min())

        # Calmar ratio: annualised return / |max_drawdown|
        calmar = ann_mean / abs(max_drawdown) if max_drawdown < 0 else np.nan

        # Skewness and kurtosis (excess kurtosis)
        if len(rets) > 2:
            centered = rets - np.mean(rets)
            m2 = np.mean(centered ** 2)
            m3 = np.mean(centered ** 3)
            m4 = np.mean(centered ** 4)
            skew = m3 / (m2 ** 1.5) if m2 > 0 else np.nan
            kurtosis = m4 / (m2 ** 2) - 3.0 if m2 > 0 else np.nan
        else:
            skew = np.nan
            kurtosis = np.nan

        records.append(
            {
                "portfolio_type": ptype,
                "ann_vol": float(ann_vol),
                "ann_sharpe": float(ann_sharpe),
                "max_drawdown": float(max_drawdown),
                "calmar": float(calmar),
                "skew": float(skew),
                "kurtosis": float(kurtosis),
            }
        )

    summary = pd.DataFrame.from_records(records)
    summary = summary.sort_values("portfolio_type").reset_index(drop=True)

    logger.info("Summary risk metrics computed.")
    return summary


def save_outputs(ts: pd.DataFrame, summary: pd.DataFrame) -> None:
    logger.info(f"Saving risk time series to {TS_OUTPUT_FILE}")
    ts.to_csv(TS_OUTPUT_FILE, index=False, encoding="utf-8")

    logger.info(f"Saving risk summary to {SUMMARY_OUTPUT_FILE}")
    summary.to_csv(SUMMARY_OUTPUT_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting risk_report_quant_v1 run (v1.0).")

    perf = load_performance()
    ts = compute_timeseries_risk(perf)
    summary = compute_summary_risk(perf)
    save_outputs(ts, summary)

    logger.info("risk_report_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()