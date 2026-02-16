# =====================================================================
# Module: quant_attribution_v1_1.py
# Quant Version: v1.1
#
# Purpose:
#   Produce factor-level daily attribution suitable for regime analytics.
#
# Description:
#   - Reads governed factor exposures timeseries
#   - Reads governed factor returns
#   - Reads tradable portfolio weights
#   - Reads governed backtest PnL (for reconciliation)
#   - Computes per-ticker, per-factor contributions
#   - Scales contributions to reconcile exactly to governed PnL
#   - Writes governed factor-level daily attribution:
#       date, ticker, factor_name, contribution, attribution_run_date
#
# Inputs:
#   C:\Quant\data\analytics\quant_factor_exposures_timeseries.csv
#       Columns:
#           - date
#           - ticker
#           - <factor columns>
#
#   C:\Quant\data\analytics\quant_factor_returns_v1.csv
#       Columns:
#           - date
#           - <factor columns> (matching exposures)
#           - factor_returns_run_date
#
#   C:\Quant\data\analytics\quant_portfolio_weights_tradable_v1_osqp.csv
#       Columns:
#           - date
#           - ticker
#           - weight_tradable_v1
#
#   C:\Quant\data\analytics\quant_backtest_pnl_v1.csv
#       Columns:
#           - date
#           - pnl
#
# Output:
#   C:\Quant\data\analytics\quant_attribution_daily_v1_1.csv
#       Columns:
#           - date
#           - ticker
#           - factor_name
#           - contribution
#           - attribution_run_date
#
# Governance:
#   - v1.0 attribution outputs remain unchanged.
#   - No schema drift: output columns fixed as above.
#   - Deterministic given governed inputs.
#   - Dates are timezone-aware ISO-8601.
# =====================================================================

import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

DATA_DIR = r"C:\Quant\data"
ANALYTICS_DIR = os.path.join(DATA_DIR, "analytics")

EXPOSURES_FILE = os.path.join(ANALYTICS_DIR, "quant_factor_exposures_timeseries.csv")
FACTOR_RETURNS_FILE = os.path.join(ANALYTICS_DIR, "quant_factor_returns_v1.csv")
WEIGHTS_FILE = os.path.join(ANALYTICS_DIR, "quant_portfolio_weights_tradable_v1_osqp.csv")
BACKTEST_FILE = os.path.join(ANALYTICS_DIR, "quant_backtest_pnl_v1.csv")

OUT_DAILY_FACTORS = os.path.join(ANALYTICS_DIR, "quant_attribution_daily_v1_1.csv")


def load_exposures() -> pd.DataFrame:
    df = pd.read_csv(EXPOSURES_FILE)
    df.columns = [c.lower() for c in df.columns]
    if "date" not in df.columns or "ticker" not in df.columns:
        raise ValueError("Exposures file must contain 'date' and 'ticker'.")
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df.dropna(subset=["date", "ticker"])
    return df


def load_factor_returns() -> pd.DataFrame:
    df = pd.read_csv(FACTOR_RETURNS_FILE)
    df.columns = [c.lower() for c in df.columns]
    if "date" not in df.columns:
        raise ValueError("Factor returns file must contain 'date'.")
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df = df.dropna(subset=["date"])
    return df


def load_weights() -> pd.DataFrame:
    df = pd.read_csv(WEIGHTS_FILE)
    df.columns = [c.lower() for c in df.columns]
    if "date" not in df.columns or "ticker" not in df.columns:
        raise ValueError("Weights file must contain 'date' and 'ticker'.")
    if "weight_tradable_v1" not in df.columns:
        raise ValueError("Weights file must contain 'weight_tradable_v1'.")
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df.rename(columns={"weight_tradable_v1": "weight"})
    df = df.dropna(subset=["date", "ticker", "weight"])
    return df


def load_backtest() -> pd.DataFrame:
    df = pd.read_csv(BACKTEST_FILE)
    df.columns = [c.lower() for c in df.columns]
    if "date" not in df.columns or "pnl" not in df.columns:
        raise ValueError("Backtest file must contain 'date' and 'pnl'.")
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df = df.dropna(subset=["date", "pnl"])
    return df[["date", "pnl"]]


def determine_factor_columns(exposures: pd.DataFrame, factor_returns: pd.DataFrame) -> list[str]:
    exp_cols = set(exposures.columns)
    ret_cols = set(factor_returns.columns)
    non_factor = {"date", "ticker", "factor_returns_run_date", "factor_run_date", "exposure_run_date"}
    candidates = (exp_cols & ret_cols) - non_factor
    factor_cols = sorted(candidates)
    if not factor_cols:
        raise ValueError("No overlapping factor columns found between exposures and factor returns.")
    return factor_cols


def compute_raw_contributions(
    exposures: pd.DataFrame,
    factor_returns: pd.DataFrame,
    weights: pd.DataFrame,
    factor_cols: list[str],
) -> pd.DataFrame:
    # Merge exposures with weights
    df = exposures.merge(weights[["date", "ticker", "weight"]], on=["date", "ticker"], how="inner")

    # Merge factor returns on date; allow suffixing for overlapping factor names
    df = df.merge(
        factor_returns[["date"] + factor_cols],
        on="date",
        how="inner",
        suffixes=("", "_ret"),
    )

    records = []

    for factor in factor_cols:
        exp_col = factor
        ret_col = factor

        # Handle possible suffixing of factor return columns
        if ret_col not in df.columns:
            if f"{ret_col}_ret" in df.columns:
                ret_col = f"{ret_col}_ret"
            elif f"{ret_col}_y" in df.columns:
                ret_col = f"{ret_col}_y"
            elif f"{ret_col}_x" in df.columns:
                ret_col = f"{ret_col}_x"
            else:
                # No usable return column for this factor; skip
                continue

        if exp_col not in df.columns:
            # No exposure column for this factor; skip
            continue

        contrib = df["weight"] * df[exp_col] * df[ret_col]
        tmp = pd.DataFrame(
            {
                "date": df["date"],
                "ticker": df["ticker"],
                "factor_name": factor,
                "raw_contribution": contrib.astype(float),
            }
        )
        records.append(tmp)

    if not records:
        raise ValueError("No factor contributions could be computed (no usable factor columns).")

    out = pd.concat(records, axis=0, ignore_index=True)
    out = out.dropna(subset=["date", "ticker", "factor_name"])
    return out


def scale_to_pnl(raw_factor_attr: pd.DataFrame, backtest: pd.DataFrame) -> pd.DataFrame:
    # Aggregate raw factor contributions to daily portfolio PnL
    daily_raw = (
        raw_factor_attr.groupby("date", as_index=False)["raw_contribution"]
        .sum()
        .rename(columns={"raw_contribution": "raw_portfolio_pnl"})
    )

    merged = backtest.merge(daily_raw, on="date", how="inner")

    def safe_scale(row: pd.Series) -> float:
        denom = row["raw_portfolio_pnl"]
        if abs(denom) > 1e-12:
            return float(row["pnl"] / denom)
        return 0.0

    merged["scale"] = merged.apply(safe_scale, axis=1)

    # Join scale back to factor-level rows
    out = raw_factor_attr.merge(merged[["date", "scale"]], on="date", how="left")
    out["scale"] = out["scale"].fillna(0.0)
    out["contribution"] = out["raw_contribution"] * out["scale"]
    return out[["date", "ticker", "factor_name", "contribution"]]


def main() -> None:
    print("quant_attribution_v1_1: Loading governed inputs...")
    exposures = load_exposures()
    factor_returns = load_factor_returns()
    weights = load_weights()
    backtest = load_backtest()

    attribution_run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    print("quant_attribution_v1_1: Determining factor columns...")
    factor_cols = determine_factor_columns(exposures, factor_returns)
    print(f"quant_attribution_v1_1: Using {len(factor_cols)} factor columns: {', '.join(factor_cols)}")

    print("quant_attribution_v1_1: Computing raw factor contributions...")
    raw_factor_attr = compute_raw_contributions(exposures, factor_returns, weights, factor_cols)

    print("quant_attribution_v1_1: Scaling contributions to governed PnL...")
    scaled = scale_to_pnl(raw_factor_attr, backtest)

    scaled = scaled.sort_values(["date", "ticker", "factor_name"]).reset_index(drop=True)
    scaled["attribution_run_date"] = attribution_run_date

    print(f"quant_attribution_v1_1: Writing factor-level daily attribution to {OUT_DAILY_FACTORS}")
    scaled.to_csv(OUT_DAILY_FACTORS, index=False, encoding="utf-8")

    print("quant_attribution_v1_1: Attribution generation complete.")


if __name__ == "__main__":
    main()