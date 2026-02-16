r"""
Quant v1.0 — portfolio_engine_quant_v1.py
Version: v1.0

1. Module name
- portfolio_engine_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Build daily long-short and long-only portfolios from a governed signal:
  - composite_signal_v1
- Compute daily and cumulative performance for:
  - long-short portfolio
  - long-only portfolio

4. Inputs
- C:\Quant\data\analytics\quant_factors_composite.csv

  Required columns:
    - date
    - ticker
    - ret                (single-period simple return, e.g. 0.01 = 1%)
    - composite_signal_v1

5. Outputs
- C:\Quant\data\analytics\quant_portfolio_weights_longshort.csv
- C:\Quant\data\analytics\quant_portfolio_weights_longonly.csv
- C:\Quant\data\analytics\quant_portfolio_performance.csv

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

8. Dependencies
- pandas
- numpy
- logging_quant_v1

9. Provenance
- This module is a governed component of Quant v1.0.
- Any modification requires version bump and architecture update.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("portfolio_engine_quant_v1")

# Files
FACTOR_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_composite.csv"
WEIGHTS_LS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_longshort.csv"
WEIGHTS_LO_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_longonly.csv"
PERF_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance.csv"

# Signal
SIGNAL_COL = "composite_signal_v1"

# Portfolio parameters
TARGET_TOTAL_NAMES = 30  # total names (long + short) where universe allows
MIN_NAMES_PER_SIDE = 5   # minimum long and short names to form a portfolio


def load_factors():
    logger.info(f"Loading factor data from {FACTOR_FILE}")
    df = pd.read_csv(FACTOR_FILE)

    required = {"date", "ticker", "ret", SIGNAL_COL}
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing required columns in factor file: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "ret", SIGNAL_COL])

    # Ensure numeric
    df["ret"] = pd.to_numeric(df["ret"], errors="coerce")
    df[SIGNAL_COL] = pd.to_numeric(df[SIGNAL_COL], errors="coerce")
    df = df.dropna(subset=["ret", SIGNAL_COL])

    logger.info(f"Loaded {len(df)} rows after dropping missing key fields.")
    return df


def build_longshort_weights(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building long-short weights.")

    def _per_date(group: pd.DataFrame) -> pd.DataFrame:
        g = group.sort_values(SIGNAL_COL, ascending=False)

        n_available = len(g)
        if n_available < 2 * MIN_NAMES_PER_SIDE:
            # Not enough names to form both sides
            g["weight"] = 0.0
            return g

        n_total = min(TARGET_TOTAL_NAMES, n_available)
        n_side = n_total // 2

        longs = g.head(n_side).copy()
        shorts = g.tail(n_side).copy()

        longs["weight"] = 1.0 / len(longs)
        shorts["weight"] = -1.0 / len(shorts)

        others = g.iloc[n_side:-n_side].copy() if n_available > 2 * n_side else g.iloc[0:0].copy()
        others["weight"] = 0.0

        out = pd.concat([longs, shorts, others], axis=0)
        return out

    ls = (
        df
        .groupby("date", group_keys=False)
        .apply(_per_date)
        .reset_index(drop=True)
    )

    dates_with_portfolio = ls.groupby("date")["weight"].apply(lambda x: (x != 0).sum())
    n_dates = (dates_with_portfolio > 0).sum()
    logger.info(
        f"Built long-short weights for {n_dates} dates with target total names "
        f"{TARGET_TOTAL_NAMES} where universe allowed."
    )

    ls = ls[["date", "ticker", "weight"]].rename(columns={"weight": "weight_longshort"})
    return ls


def build_longonly_weights(ls_weights: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building long-only weights from long-short weights.")

    def _normalize(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        g["weight_longonly"] = 0.0
        longs = g[g["weight_longshort"] > 0].copy()
        if longs.empty:
            return g
        total = longs["weight_longshort"].sum()
        if total == 0:
            return g
        longs["weight_longonly"] = longs["weight_longshort"] / total
        g.update(longs)
        return g

    lo = (
        ls_weights
        .groupby("date", group_keys=False)
        .apply(_normalize)
        .reset_index(drop=True)
    )

    n_dates = lo["date"].nunique()
    logger.info(
        f"Built long-only weights for {n_dates} dates by renormalizing long-side weights to sum to 1.0."
    )

    return lo[["date", "ticker", "weight_longonly"]]


def compute_performance(df_factors: pd.DataFrame,
                        ls_weights: pd.DataFrame,
                        lo_weights: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing portfolio performance.")

    base = df_factors[["date", "ticker", "ret"]].copy()

    # Merge weights
    merged = base.merge(ls_weights, on=["date", "ticker"], how="left")
    merged = merged.merge(lo_weights, on=["date", "ticker"], how="left")

    merged["weight_longshort"] = merged["weight_longshort"].fillna(0.0)
    merged["weight_longonly"] = merged["weight_longonly"].fillna(0.0)

    # Daily returns
    daily_ls = (
        merged
        .groupby("date", as_index=False)
        .apply(lambda g: pd.Series({
            "daily_return": np.sum(g["weight_longshort"] * g["ret"])
        }))
    )
    daily_ls["portfolio_type"] = "longshort"

    daily_lo = (
        merged
        .groupby("date", as_index=False)
        .apply(lambda g: pd.Series({
            "daily_return": np.sum(g["weight_longonly"] * g["ret"])
        }))
    )
    daily_lo["portfolio_type"] = "longonly"

    perf = pd.concat([daily_ls, daily_lo], axis=0, ignore_index=True)
    perf = perf.sort_values(["portfolio_type", "date"]).reset_index(drop=True)

    # Cumulative return as running sum of daily_return (consistent with existing file)
    perf["cumulative_return"] = (
        perf
        .groupby("portfolio_type")["daily_return"]
        .cumsum()
    )

    logger.info("Portfolio performance computed for long-short and long-only portfolios.")
    return perf[["date", "portfolio_type", "daily_return", "cumulative_return"]]


def save_outputs(ls_weights: pd.DataFrame,
                 lo_weights: pd.DataFrame,
                 perf: pd.DataFrame) -> None:
    logger.info(f"Saving long-short weights to {WEIGHTS_LS_FILE}")
    ls_weights.to_csv(WEIGHTS_LS_FILE, index=False, encoding="utf-8")

    logger.info(f"Saving long-only weights to {WEIGHTS_LO_FILE}")
    lo_weights.to_csv(WEIGHTS_LO_FILE, index=False, encoding="utf-8")

    logger.info(f"Saving performance to {PERF_FILE}")
    perf.to_csv(PERF_FILE, index=False, encoding="utf-8")


def main():
    logger.info(
        "Starting portfolio_engine_quant_v1 run (v1.0, run hot, long-short + long-only, signal=composite_signal_v1)."
    )

    df = load_factors()
    ls_weights = build_longshort_weights(df)
    lo_weights = build_longonly_weights(ls_weights)
    perf = compute_performance(df, ls_weights, lo_weights)
    save_outputs(ls_weights, lo_weights, perf)

    logger.info("portfolio_engine_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()