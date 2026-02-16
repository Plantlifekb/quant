r"""
Quant v1.0 — Alpha Engine v1

Builds a composite expected-return signal from factor data and writes:
    data/analytics/quant_expected_returns_timeseries.csv

This file is then consumed by:
- position_sizing_engine_quant_v1_osqp.py
- quant_backtest_pnl_v1.py
"""

import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

FACTOR_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_ensemble_risk_v1.csv"
OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_expected_returns_timeseries.csv"


def zscore_series(x: pd.Series) -> pd.Series:
    mu = x.mean()
    sigma = x.std()
    if sigma == 0 or np.isnan(sigma):
        return pd.Series(0.0, index=x.index)
    return (x - mu) / sigma


def build_alpha():
    df = pd.read_csv(FACTOR_FILE)
    df["date"] = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str).str.upper()

    # Ensure factor columns exist; if not, create zeros
    for col in ["size_factor", "vol_factor", "liquidity_factor"]:
        if col not in df.columns:
            df[col] = 0.0

    # Cross-sectional z-scores per date
    df = df.sort_values(["date", "ticker"])
    df["size_z"] = df.groupby("date")["size_factor"].transform(zscore_series)
    df["vol_z"] = df.groupby("date")["vol_factor"].transform(zscore_series)
    df["liq_z"] = df.groupby("date")["liquidity_factor"].transform(zscore_series)

    # Composite alpha:
    # - smaller size (negative size_z) is good
    # - lower vol (negative vol_z) is good
    # - higher liquidity (positive liq_z) is good
    alpha = (
        0.4 * (-df["size_z"]) +
        0.3 * (-df["vol_z"]) +
        0.3 * (df["liq_z"])
    )

    # Optional: rescale alpha to a reasonable range
    # Here we just clip extreme values
    alpha = alpha.clip(lower=-3.0, upper=3.0)

    df_out = df[["date", "ticker"]].copy()
    df_out["expected_return"] = alpha

    return df_out


def main():
    print("Building composite alpha from factor file...")
    df_alpha = build_alpha()
    df_alpha.to_csv(OUT_FILE, index=False)
    print(f"Saved expected returns to: {OUT_FILE}")


if __name__ == "__main__":
    main()