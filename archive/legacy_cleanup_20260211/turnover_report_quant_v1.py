r"""
Quant v1.0 — turnover_report_quant_v1.py
Version: v1.0

1. Module name
- turnover_report_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Compute governed turnover and simple cost-adjusted performance diagnostics for:
  - Raw ensemble portfolios (if desired, future extension)
  - Risk-neutral ensemble portfolios (current focus)
- For each portfolio_type ("longshort", "longonly"):
  - Compute daily turnover based on changes in weights
  - Compute average daily turnover
  - Compute simple transaction-cost-adjusted daily returns and cumulative returns
- Output:
  - Time series of turnover and cost-adjusted returns
  - Summary statistics by portfolio_type

4. Inputs
- C:\Quant\data\analytics\quant_portfolio_weights_ensemble_risk_longshort.csv
- C:\Quant\data\analytics\quant_portfolio_weights_ensemble_risk_longonly.csv
- C:\Quant\data\analytics\quant_portfolio_performance_ensemble_risk.csv

  Required columns:

  weights_longshort:
    - date
    - ticker
    - weight_longshort

  weights_longonly:
    - date
    - ticker
    - weight_longonly

  performance_ensemble_risk:
    - date
    - portfolio_type   ("longshort" / "longonly")
    - daily_return
    - cumulative_return

5. Outputs
- C:\Quant\data\analytics\quant_turnover_timeseries_ensemble_risk.csv
- C:\Quant\data\analytics\quant_turnover_summary_ensemble_risk.csv

  turnover_timeseries columns:
    - date
    - portfolio_type
    - gross_turnover        (sum |w_t - w_{t-1}| over names)
    - cost_adjusted_return  (daily_return - cost_per_unit * gross_turnover)

  turnover_summary columns:
    - portfolio_type
    - avg_daily_turnover
    - median_daily_turnover
    - cost_per_unit
    - avg_daily_return
    - avg_cost_adjusted_return
    - total_return
    - total_cost_adjusted_return

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

logger = get_logger("turnover_report_quant_v1")

# Files
W_LS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort.csv"
W_LO_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longonly.csv"
PERF_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance_ensemble_risk.csv"

TS_OUTPUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_turnover_timeseries_ensemble_risk.csv"
SUMMARY_OUTPUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_turnover_summary_ensemble_risk.csv"

# Simple, governed transaction cost assumption:
# cost_per_unit is the cost per 1 unit of turnover (sum |Δw|).
COST_PER_UNIT = 0.001  # 10 bps per 100% notional turnover


def load_weights_and_perf() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info("Loading risk-neutral portfolio weights and performance.")

    w_ls = pd.read_csv(W_LS_FILE)
    w_lo = pd.read_csv(W_LO_FILE)
    perf = pd.read_csv(PERF_FILE)

    # Dates
    w_ls["date"] = pd.to_datetime(w_ls["date"], errors="coerce")
    w_lo["date"] = pd.to_datetime(w_lo["date"], errors="coerce")
    perf["date"] = pd.to_datetime(perf["date"], errors="coerce")

    w_ls = w_ls.dropna(subset=["date", "ticker", "weight_longshort"])
    w_lo = w_lo.dropna(subset=["date", "ticker", "weight_longonly"])
    perf = perf.dropna(subset=["date", "portfolio_type", "daily_return"])

    w_ls["weight_longshort"] = pd.to_numeric(w_ls["weight_longshort"], errors="coerce")
    w_lo["weight_longonly"] = pd.to_numeric(w_lo["weight_longonly"], errors="coerce")
    perf["daily_return"] = pd.to_numeric(perf["daily_return"], errors="coerce")

    w_ls = w_ls.dropna(subset=["weight_longshort"])
    w_lo = w_lo.dropna(subset=["weight_longonly"])
    perf = perf.dropna(subset=["daily_return"])

    logger.info(
        f"Loaded {len(w_ls)} long-short weight rows, "
        f"{len(w_lo)} long-only weight rows, "
        f"and {len(perf)} performance rows."
    )

    return w_ls, w_lo, perf


def _compute_turnover_for_weights(
    weights: pd.DataFrame,
    weight_col: str,
    portfolio_type: str,
) -> pd.DataFrame:
    """
    Compute daily gross turnover for a given weights DataFrame.
    Turnover_t = sum_i |w_{i,t} - w_{i,t-1}| over all names.
    """
    logger.info(f"Computing turnover for portfolio_type={portfolio_type}, weight_col={weight_col}.")

    df = weights.copy()
    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)

    records = []

    # Group by date, but we need previous date's weights, so we work on full panel
    dates = sorted(df["date"].unique())

    # Build a mapping date -> weights (ticker, weight)
    by_date = {d: g.set_index("ticker")[weight_col] for d, g in df.groupby("date")}

    prev_weights = None
    for d in dates:
        w_today = by_date[d]

        if prev_weights is None:
            # First date: define turnover as sum |w_t| (entering positions)
            gross_turnover = float(np.sum(np.abs(w_today.values)))
        else:
            # Align tickers union
            all_tickers = sorted(set(prev_weights.index) | set(w_today.index))
            prev_vec = prev_weights.reindex(all_tickers).fillna(0.0).values
            curr_vec = w_today.reindex(all_tickers).fillna(0.0).values
            gross_turnover = float(np.sum(np.abs(curr_vec - prev_vec)))

        records.append(
            {
                "date": d,
                "portfolio_type": portfolio_type,
                "gross_turnover": gross_turnover,
            }
        )

        prev_weights = w_today

    out = pd.DataFrame.from_records(records)
    out = out.sort_values(["portfolio_type", "date"]).reset_index(drop=True)

    logger.info(f"Computed {len(out)} daily turnover rows for {portfolio_type}.")
    return out


def compute_turnover_timeseries(
    w_ls: pd.DataFrame,
    w_lo: pd.DataFrame,
    perf: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Computing turnover time series and cost-adjusted returns.")

    # Turnover for long-short
    ts_ls = _compute_turnover_for_weights(
        weights=w_ls,
        weight_col="weight_longshort",
        portfolio_type="longshort",
    )

    # Turnover for long-only
    ts_lo = _compute_turnover_for_weights(
        weights=w_lo,
        weight_col="weight_longonly",
        portfolio_type="longonly",
    )

    ts = pd.concat([ts_ls, ts_lo], axis=0, ignore_index=True)

    # Merge with performance
    perf_use = perf[["date", "portfolio_type", "daily_return"]].copy()
    merged = ts.merge(
        perf_use,
        on=["date", "portfolio_type"],
        how="left",
    )

    merged["daily_return"] = merged["daily_return"].fillna(0.0)

    # Cost-adjusted return
    merged["cost_per_unit"] = COST_PER_UNIT
    merged["cost_adjusted_return"] = (
        merged["daily_return"] - merged["cost_per_unit"] * merged["gross_turnover"]
    )

    merged = merged.sort_values(["portfolio_type", "date"]).reset_index(drop=True)

    logger.info("Turnover time series and cost-adjusted returns computed.")
    return merged[
        [
            "date",
            "portfolio_type",
            "gross_turnover",
            "cost_per_unit",
            "daily_return",
            "cost_adjusted_return",
        ]
    ]


def compute_turnover_summary(ts: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing turnover summary statistics.")

    if ts.empty:
        logger.warning("Turnover time series is empty; summary will be empty.")
        return pd.DataFrame(
            columns=[
                "portfolio_type",
                "avg_daily_turnover",
                "median_daily_turnover",
                "cost_per_unit",
                "avg_daily_return",
                "avg_cost_adjusted_return",
                "total_return",
                "total_cost_adjusted_return",
            ]
        )

    grouped = ts.groupby("portfolio_type", as_index=False).agg(
        avg_daily_turnover=("gross_turnover", "mean"),
        median_daily_turnover=("gross_turnover", "median"),
        cost_per_unit=("cost_per_unit", "first"),
        avg_daily_return=("daily_return", "mean"),
        avg_cost_adjusted_return=("cost_adjusted_return", "mean"),
        total_return=("daily_return", "sum"),
        total_cost_adjusted_return=("cost_adjusted_return", "sum"),
    )

    grouped = grouped.sort_values("portfolio_type").reset_index(drop=True)

    logger.info("Turnover summary statistics computed.")
    return grouped


def save_outputs(ts: pd.DataFrame, summary: pd.DataFrame) -> None:
    logger.info(f"Saving turnover time series to {TS_OUTPUT_FILE}")
    ts.to_csv(TS_OUTPUT_FILE, index=False, encoding="utf-8")

    logger.info(f"Saving turnover summary to {SUMMARY_OUTPUT_FILE}")
    summary.to_csv(SUMMARY_OUTPUT_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting turnover_report_quant_v1 run (v1.0).")

    w_ls, w_lo, perf = load_weights_and_perf()
    ts = compute_turnover_timeseries(w_ls, w_lo, perf)
    summary = compute_turnover_summary(ts)
    save_outputs(ts, summary)

    logger.info("turnover_report_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()