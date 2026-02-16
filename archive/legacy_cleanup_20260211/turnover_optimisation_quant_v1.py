r"""
Quant v1.0 — turnover_optimisation_quant_v1.py
Version: v1.0

1. Module name
- turnover_optimisation_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Reduce portfolio turnover in a governed, transparent way by:
  - Taking existing daily weights (long-short and long-only)
  - Applying a simple, explicit rebalance schedule
  - Holding weights constant between rebalance dates
  - Recomputing portfolio performance from the lower-turnover weights

4. Inputs
- C:\Quant\data\analytics\quant_factors_composite.csv

  Required columns:
    - date
    - ticker
    - ret

- C:\Quant\data\analytics\quant_portfolio_weights_longshort.csv

  Required columns:
    - date
    - ticker
    - weight_longshort

- C:\Quant\data\analytics\quant_portfolio_weights_longonly.csv

  Required columns:
    - date
    - ticker
    - weight_longonly

5. Outputs
- C:\Quant\data\analytics\quant_portfolio_weights_longshort_to.csv
- C:\Quant\data\analytics\quant_portfolio_weights_longonly_to.csv
- C:\Quant\data\analytics\quant_portfolio_performance_to.csv

  performance_to columns:
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

logger = get_logger("turnover_optimisation_quant_v1")

# Files
FACTOR_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_composite.csv"
W_LS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_longshort.csv"
W_LO_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_longonly.csv"

W_LS_TO_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_longshort_to.csv"
W_LO_TO_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_longonly_to.csv"
PERF_TO_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance_to.csv"

# Turnover parameters
# Rebalance every N trading days; between rebalances, hold previous weights.
REBALANCE_FREQUENCY_DAYS = 5  # e.g. weekly-ish


def load_factors() -> pd.DataFrame:
    logger.info(f"Loading factor data from {FACTOR_FILE}")
    df = pd.read_csv(FACTOR_FILE)

    required = {"date", "ticker", "ret"}
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing required columns in factor file: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "ret"])

    df["ret"] = pd.to_numeric(df["ret"], errors="coerce")
    df = df.dropna(subset=["ret"])

    logger.info(f"Loaded {len(df)} factor rows after cleaning.")
    return df[["date", "ticker", "ret"]]


def load_weights(file_path: Path, weight_col: str) -> pd.DataFrame:
    logger.info(f"Loading weights from {file_path}")
    df = pd.read_csv(file_path)

    required = {"date", "ticker", weight_col}
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing required columns in weights file {file_path}: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", weight_col])

    df[weight_col] = pd.to_numeric(df[weight_col], errors="coerce")
    df = df.dropna(subset=[weight_col])

    logger.info(f"Loaded {len(df)} rows from {file_path} after cleaning.")
    return df[["date", "ticker", weight_col]]


def apply_rebalance_schedule(weights: pd.DataFrame, weight_col: str) -> pd.DataFrame:
    """
    Simple, explicit turnover control:
    - On rebalance dates (every REBALANCE_FREQUENCY_DAYS), use original weights.
    - On non-rebalance dates, hold previous optimised weights.
    """
    logger.info(
        f"Applying rebalance schedule (every {REBALANCE_FREQUENCY_DAYS} days) for {weight_col}."
    )

    # Pivot to date x ticker matrix
    pivot = (
        weights
        .pivot(index="date", columns="ticker", values=weight_col)
        .sort_index()
    )

    dates = pivot.index.to_list()
    optimised = pivot.copy() * np.nan

    for i, dt in enumerate(dates):
        if i == 0:
            # First date: always rebalance (use original weights)
            optimised.loc[dt] = pivot.loc[dt]
        else:
            if i % REBALANCE_FREQUENCY_DAYS == 0:
                # Rebalance date: use original weights
                optimised.loc[dt] = pivot.loc[dt]
            else:
                # Non-rebalance date: hold previous optimised weights
                optimised.loc[dt] = optimised.iloc[i - 1]

    # Back to long format
    optimised_long = (
        optimised
        .reset_index()
        .melt(id_vars="date", var_name="ticker", value_name=weight_col)
        .dropna(subset=[weight_col])
        .sort_values(["date", "ticker"])
        .reset_index(drop=True)
    )

    logger.info(
        f"Rebalance schedule applied for {weight_col}: "
        f"{len(optimised_long)} rows in optimised weights."
    )
    return optimised_long


def compute_performance(
    factors: pd.DataFrame,
    w_ls_to: pd.DataFrame,
    w_lo_to: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Computing performance from turnover-optimised weights.")

    base = factors.copy()

    # Long-short
    ls = base.merge(w_ls_to, on=["date", "ticker"], how="left")
    ls["weight_longshort"] = ls["weight_longshort"].fillna(0.0)

    daily_ls = (
        ls.groupby("date", as_index=False)
        .apply(lambda g: pd.Series({"daily_return": np.sum(g["weight_longshort"] * g["ret"])}))
    )
    daily_ls["portfolio_type"] = "longshort"

    # Long-only
    lo = base.merge(w_lo_to, on=["date", "ticker"], how="left")
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

    logger.info("Turnover-optimised performance computed for long-short and long-only.")
    return perf[["date", "portfolio_type", "daily_return", "cumulative_return"]]


def save_outputs(
    w_ls_to: pd.DataFrame,
    w_lo_to: pd.DataFrame,
    perf_to: pd.DataFrame,
) -> None:
    logger.info(f"Saving turnover-optimised long-short weights to {W_LS_TO_FILE}")
    w_ls_to.to_csv(W_LS_TO_FILE, index=False, encoding="utf-8")

    logger.info(f"Saving turnover-optimised long-only weights to {W_LO_TO_FILE}")
    w_lo_to.to_csv(W_LO_TO_FILE, index=False, encoding="utf-8")

    logger.info(f"Saving turnover-optimised performance to {PERF_TO_FILE}")
    perf_to.to_csv(PERF_TO_FILE, index=False, encoding="utf-8")


def main():
    logger.info(
        "Starting turnover_optimisation_quant_v1 run "
        f"(v1.0, rebalance every {REBALANCE_FREQUENCY_DAYS} days)."
    )

    factors = load_factors()
    w_ls = load_weights(W_LS_FILE, "weight_longshort")
    w_lo = load_weights(W_LO_FILE, "weight_longonly")

    w_ls_to = apply_rebalance_schedule(w_ls, "weight_longshort")
    w_lo_to = apply_rebalance_schedule(w_lo, "weight_longonly")

    perf_to = compute_performance(factors, w_ls_to, w_lo_to)
    save_outputs(w_ls_to, w_lo_to, perf_to)

    logger.info("turnover_optimisation_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()