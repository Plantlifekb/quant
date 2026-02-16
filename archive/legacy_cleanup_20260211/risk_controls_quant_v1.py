r"""
Quant v1.0 — risk_controls_quant_v1.py
Version: v1.0

1. Module name
- risk_controls_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Apply lightweight, transparent risk controls to portfolio weights:
  - Enforce max position size
  - Enforce sector caps (if sector data available)
  - Renormalise weights after constraints
  - Recompute portfolio performance from risk-controlled weights

4. Inputs
- C:\Quant\data\analytics\quant_factors_composite.csv

  Required columns:
    - date
    - ticker
    - ret

  Optional columns (for sector caps):
    - sector

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
- C:\Quant\data\analytics\quant_portfolio_weights_longshort_rc.csv
- C:\Quant\data\analytics\quant_portfolio_weights_longonly_rc.csv
- C:\Quant\data\analytics\quant_portfolio_performance_rc.csv

  performance_rc columns:
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

logger = get_logger("risk_controls_quant_v1")

# Files
FACTOR_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_composite.csv"
W_LS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_longshort.csv"
W_LO_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_longonly.csv"

W_LS_RC_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_longshort_rc.csv"
W_LO_RC_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_longonly_rc.csv"
PERF_RC_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance_rc.csv"

# Risk control parameters
MAX_POSITION_LONG = 0.10   # 10% max long weight per name
MAX_POSITION_SHORT = -0.10 # -10% max short weight per name
MAX_SECTOR_WEIGHT = 0.30   # 30% max sector weight (if sector data available)


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

    # sector is optional
    if "sector" not in df.columns:
        logger.info("No 'sector' column found in factor file; sector caps will be skipped.")
        df["sector"] = "unknown"

    logger.info(f"Loaded {len(df)} factor rows after cleaning.")
    return df[["date", "ticker", "ret", "sector"]]


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


def apply_position_limits(group: pd.DataFrame, weight_col: str) -> pd.DataFrame:
    g = group.copy()
    w = g[weight_col]

    # Separate long and short
    longs = w.clip(upper=MAX_POSITION_LONG)
    shorts = w.clip(lower=MAX_POSITION_SHORT)

    # Combine: for positive weights, use longs; for negative, use shorts; zero stays zero
    adjusted = np.where(w > 0, longs, np.where(w < 0, shorts, 0.0))
    g[weight_col] = adjusted

    return g


def apply_sector_caps(group: pd.DataFrame, weight_col: str) -> pd.DataFrame:
    g = group.copy()

    # If all sectors are 'unknown', skip sector caps
    if g["sector"].nunique() == 1 and g["sector"].iloc[0] == "unknown":
        return g

    # Compute sector weights
    sector_weights = g.groupby("sector")[weight_col].sum()

    # For each sector above cap, scale down proportionally
    for sector, sw in sector_weights.items():
        if sw > MAX_SECTOR_WEIGHT + 1e-12:
            scale = MAX_SECTOR_WEIGHT / sw
            mask = g["sector"] == sector
            g.loc[mask, weight_col] = g.loc[mask, weight_col] * scale

    return g


def renormalise_weights(group: pd.DataFrame, weight_col: str, portfolio_type: str) -> pd.DataFrame:
    g = group.copy()
    w = g[weight_col]

    if portfolio_type == "longonly":
        # Long-only: ensure sum of positive weights = 1.0
        pos = w[w > 0]
        total = pos.sum()
        if total > 0:
            scale = 1.0 / total
            g.loc[w > 0, weight_col] = w[w > 0] * scale
        else:
            # No positive weights: leave as is (all zero)
            pass
    elif portfolio_type == "longshort":
        # Long-short: enforce sum(long) = 1, sum(short) = -1 if possible
        longs = w[w > 0]
        shorts = w[w < 0]

        long_sum = longs.sum()
        short_sum = shorts.sum()

        if long_sum > 0:
            long_scale = 1.0 / long_sum
            g.loc[w > 0, weight_col] = w[w > 0] * long_scale

        if short_sum < 0:
            short_scale = -1.0 / short_sum
            g.loc[w < 0, weight_col] = w[w < 0] * short_scale

    return g


def apply_risk_controls(
    factors: pd.DataFrame,
    weights: pd.DataFrame,
    weight_col: str,
    portfolio_type: str,
) -> pd.DataFrame:
    logger.info(
        f"Applying risk controls for {portfolio_type} on column {weight_col}: "
        f"max_pos_long={MAX_POSITION_LONG}, max_pos_short={MAX_POSITION_SHORT}, "
        f"max_sector_weight={MAX_SECTOR_WEIGHT}."
    )

    # Merge sector info
    merged = weights.merge(
        factors[["date", "ticker", "sector"]],
        on=["date", "ticker"],
        how="left",
    )

    # Apply per-date controls
    def _per_date(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        g = apply_position_limits(g, weight_col)
        g = apply_sector_caps(g, weight_col)
        g = renormalise_weights(g, weight_col, portfolio_type)
        return g

    out = (
        merged
        .groupby("date", group_keys=False)
        .apply(_per_date)
        .reset_index(drop=True)
    )

    logger.info(
        f"Risk controls applied for {portfolio_type}: {len(out)} rows in risk-controlled weights."
    )
    return out[["date", "ticker", weight_col]]


def compute_performance(
    factors: pd.DataFrame,
    w_ls_rc: pd.DataFrame,
    w_lo_rc: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Computing performance from risk-controlled weights.")

    base = factors.copy()

    # Long-short
    ls = base.merge(w_ls_rc, on=["date", "ticker"], how="left")
    ls["weight_longshort"] = ls["weight_longshort"].fillna(0.0)

    daily_ls = (
        ls.groupby("date", as_index=False)
        .apply(lambda g: pd.Series({"daily_return": np.sum(g["weight_longshort"] * g["ret"])}))
    )
    daily_ls["portfolio_type"] = "longshort"

    # Long-only
    lo = base.merge(w_lo_rc, on=["date", "ticker"], how="left")
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

    logger.info("Risk-controlled performance computed for long-short and long-only.")
    return perf[["date", "portfolio_type", "daily_return", "cumulative_return"]]


def save_outputs(
    w_ls_rc: pd.DataFrame,
    w_lo_rc: pd.DataFrame,
    perf_rc: pd.DataFrame,
) -> None:
    logger.info(f"Saving risk-controlled long-short weights to {W_LS_RC_FILE}")
    w_ls_rc.to_csv(W_LS_RC_FILE, index=False, encoding="utf-8")

    logger.info(f"Saving risk-controlled long-only weights to {W_LO_RC_FILE}")
    w_lo_rc.to_csv(W_LO_RC_FILE, index=False, encoding="utf-8")

    logger.info(f"Saving risk-controlled performance to {PERF_RC_FILE}")
    perf_rc.to_csv(PERF_RC_FILE, index=False, encoding="utf-8")


def main():
    logger.info(
        "Starting risk_controls_quant_v1 run "
        f"(v1.0, max_pos_long={MAX_POSITION_LONG}, max_pos_short={MAX_POSITION_SHORT}, "
        f"max_sector_weight={MAX_SECTOR_WEIGHT})."
    )

    factors = load_factors()
    w_ls = load_weights(W_LS_FILE, "weight_longshort")
    w_lo = load_weights(W_LO_FILE, "weight_longonly")

    w_ls_rc = apply_risk_controls(factors, w_ls, "weight_longshort", "longshort")
    w_lo_rc = apply_risk_controls(factors, w_lo, "weight_longonly", "longonly")

    perf_rc = compute_performance(factors, w_ls_rc, w_lo_rc)
    save_outputs(w_ls_rc, w_lo_rc, perf_rc)

    logger.info("risk_controls_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()