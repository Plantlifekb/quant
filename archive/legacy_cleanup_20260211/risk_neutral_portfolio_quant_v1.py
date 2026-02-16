r"""
Quant v1.0 — risk_neutral_portfolio_quant_v1.py
Version: v1.0

1. Module name
- risk_neutral_portfolio_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Take the factor-neutralised ensemble signal (ensemble_signal_v1_resid) and turn it into:
  - Long-short risk-neutral weights
  - Long-only risk-neutral weights
  - Portfolio performance for both
- Parallel to ensemble_portfolio_quant_v1, but using ensemble_signal_v1_resid.

4. Inputs
- C:\Quant\data\analytics\quant_factors_ensemble_risk_v1.csv

  Required columns:
    - date
    - ticker
    - ret
    - ensemble_signal_v1_resid

5. Outputs
- C:\Quant\data\analytics\quant_portfolio_weights_ensemble_risk_longshort.csv
- C:\Quant\data\analytics\quant_portfolio_weights_ensemble_risk_longonly.csv
- C:\Quant\data\analytics\quant_portfolio_performance_ensemble_risk.csv

  performance_ensemble_risk columns:
    - date
    - portfolio_type   ("longshort" / "longonly")
    - daily_return
    - cumulative_return

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

logger = get_logger("risk_neutral_portfolio_quant_v1")

# Files
RISK_ENSEMBLE_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_ensemble_risk_v1.csv"

W_LS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort.csv"
W_LO_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longonly.csv"
PERF_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance_ensemble_risk.csv"

# Portfolio construction parameters
TOP_QUANTILE = 0.2   # top 20% long
BOTTOM_QUANTILE = 0.2  # bottom 20% short


def load_risk_ensemble() -> pd.DataFrame:
    logger.info(f"Loading risk-model ensemble factors from {RISK_ENSEMBLE_FILE}")
    df = pd.read_csv(RISK_ENSEMBLE_FILE)

    required = {"date", "ticker", "ret", "ensemble_signal_v1_resid"}
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing required columns in risk ensemble file: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "ret", "ensemble_signal_v1_resid"])

    df["ret"] = pd.to_numeric(df["ret"], errors="coerce")
    df["ensemble_signal_v1_resid"] = pd.to_numeric(
        df["ensemble_signal_v1_resid"], errors="coerce"
    )
    df = df.dropna(subset=["ret", "ensemble_signal_v1_resid"])

    logger.info(f"Loaded {len(df)} risk-ensemble rows after cleaning.")
    return df[["date", "ticker", "ret", "ensemble_signal_v1_resid"]]


def build_longshort_weights(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(
        "Building risk-neutral ensemble long-short weights using top/bottom quantiles: "
        f"top={TOP_QUANTILE}, bottom={BOTTOM_QUANTILE}."
    )

    def _per_date(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        n = len(g)
        if n == 0:
            g["weight_longshort"] = 0.0
            return g

        g = g.sort_values("ensemble_signal_v1_resid", ascending=False)

        n_long = max(int(np.floor(TOP_QUANTILE * n)), 1)
        n_short = max(int(np.floor(BOTTOM_QUANTILE * n)), 1)

        g["weight_longshort"] = 0.0

        # Longs: top residual signals
        long_idx = g.index[:n_long]
        g.loc[long_idx, "weight_longshort"] = 1.0 / n_long

        # Shorts: bottom residual signals
        short_idx = g.index[-n_short:]
        g.loc[short_idx, "weight_longshort"] = -1.0 / n_short

        return g

    out = (
        df.groupby("date", group_keys=False)
        .apply(_per_date)
        .reset_index(drop=True)
    )

    logger.info("Risk-neutral ensemble long-short weights built.")
    return out[["date", "ticker", "weight_longshort"]]


def build_longonly_weights(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(
        "Building risk-neutral ensemble long-only weights using top quantile: "
        f"top={TOP_QUANTILE}."
    )

    def _per_date(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        n = len(g)
        if n == 0:
            g["weight_longonly"] = 0.0
            return g

        g = g.sort_values("ensemble_signal_v1_resid", ascending=False)

        n_long = max(int(np.floor(TOP_QUANTILE * n)), 1)

        g["weight_longonly"] = 0.0
        long_idx = g.index[:n_long]
        g.loc[long_idx, "weight_longonly"] = 1.0 / n_long

        return g

    out = (
        df.groupby("date", group_keys=False)
        .apply(_per_date)
        .reset_index(drop=True)
    )

    logger.info("Risk-neutral ensemble long-only weights built.")
    return out[["date", "ticker", "weight_longonly"]]


def compute_performance(
    df: pd.DataFrame,
    w_ls: pd.DataFrame,
    w_lo: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Computing risk-neutral ensemble portfolio performance for long-short and long-only.")

    base = df.copy()

    # Long-short
    ls = base.merge(w_ls, on=["date", "ticker"], how="left")
    ls["weight_longshort"] = ls["weight_longshort"].fillna(0.0)

    daily_ls = (
        ls.groupby("date", as_index=False)
        .apply(lambda g: pd.Series({"daily_return": np.sum(g["weight_longshort"] * g["ret"])}))
    )
    daily_ls["portfolio_type"] = "longshort"

    # Long-only
    lo = base.merge(w_lo, on=["date", "ticker"], how="left")
    lo["weight_longonly"] = lo["weight_longonly"].fillna(0.0)

    daily_lo = (
        lo.groupby("date", as_index=False)
        .apply(lambda g: pd.Series({"daily_return": np.sum(g["weight_longonly"] * g["ret"])}))
    )
    daily_lo["portfolio_type"] = "longonly"

    perf = pd.concat([daily_ls, daily_lo], axis=0, ignore_index=True)
    perf = perf.sort_values(["portfolio_type", "date"]).reset_index(drop=True)

    perf["cumulative_return"] = (
        perf.groupby("portfolio_type")["daily_return"].cumsum()
    )

    logger.info("Risk-neutral ensemble portfolio performance computed.")
    return perf[["date", "portfolio_type", "daily_return", "cumulative_return"]]


def save_outputs(
    w_ls: pd.DataFrame,
    w_lo: pd.DataFrame,
    perf: pd.DataFrame,
) -> None:
    logger.info(f"Saving risk-neutral ensemble long-short weights to {W_LS_FILE}")
    w_ls.to_csv(W_LS_FILE, index=False, encoding="utf-8")

    logger.info(f"Saving risk-neutral ensemble long-only weights to {W_LO_FILE}")
    w_lo.to_csv(W_LO_FILE, index=False, encoding="utf-8")

    logger.info(f"Saving risk-neutral ensemble portfolio performance to {PERF_FILE}")
    perf.to_csv(PERF_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting risk_neutral_portfolio_quant_v1 run (v1.0).")

    df = load_risk_ensemble()
    w_ls = build_longshort_weights(df)
    w_lo = build_longonly_weights(df)
    perf = compute_performance(df, w_ls, w_lo)
    save_outputs(w_ls, w_lo, perf)

    logger.info("risk_neutral_portfolio_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()