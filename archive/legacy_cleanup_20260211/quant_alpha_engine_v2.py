r"""
Quant v1.0 — Alpha Engine v2 (price-based)

Builds a composite expected-return signal from 5y price history:
- Uses adj_close from ingestion_5years.csv
- Computes:
    * daily returns
    * 1m, 3m, 6m, 12m momentum
    * 20d and 60d volatility
    * simple liquidity proxy (log dollar volume)
- Combines into a composite alpha
- Writes:
    data/analytics/quant_expected_returns_timeseries.csv

This file is then consumed by:
- position_sizing_engine_quant_v1_osqp.py
- quant_backtest_pnl_v1.py
"""

import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INGEST_FILE = PROJECT_ROOT / "data" / "ingestion" / "ingestion_5years.csv"
OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_expected_returns_timeseries.csv"


def compute_daily_returns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["ticker", "date"])
    df["ret_1d"] = df.groupby("ticker")["adj_close"].pct_change()
    return df


def rolling_return(series: pd.Series, window: int) -> pd.Series:
    # (P_t / P_{t-window}) - 1
    return series / series.shift(window) - 1.0


def zscore(x: pd.Series) -> pd.Series:
    mu = x.mean()
    sigma = x.std()
    if sigma == 0 or np.isnan(sigma):
        return pd.Series(0.0, index=x.index)
    return (x - mu) / sigma


def build_alpha() -> pd.DataFrame:
    df = pd.read_csv(INGEST_FILE)
    df["date"] = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str).str.upper()

    # Keep only what we need
    df = df[["date", "ticker", "adj_close", "volume"]].copy()

    # Compute daily returns
    df = compute_daily_returns(df)

    # Momentum windows (approx trading days)
    windows = {
        "mom_1m": 21,
        "mom_3m": 63,
        "mom_6m": 126,
        "mom_12m": 252,
    }

    for name, win in windows.items():
        df[name] = df.groupby("ticker")["adj_close"].transform(
            lambda x, w=win: rolling_return(x, w)
        )

    # Volatility (20d, 60d) on daily returns
    df["vol_20d"] = df.groupby("ticker")["ret_1d"].transform(
        lambda x: x.rolling(20).std()
    )
    df["vol_60d"] = df.groupby("ticker")["ret_1d"].transform(
        lambda x: x.rolling(60).std()
    )

    # Liquidity proxy: log dollar volume (price * volume)
    df["dollar_vol"] = df["adj_close"] * df["volume"]
    df["liq_log_dv"] = np.log(df["dollar_vol"].replace(0, np.nan))
    df["liq_log_dv"] = df["liq_log_dv"].fillna(df["liq_log_dv"].median())

    # Cross-sectional z-scores per date
    df = df.sort_values(["date", "ticker"])

    for col in ["mom_1m", "mom_3m", "mom_6m", "mom_12m", "vol_20d", "vol_60d", "liq_log_dv"]:
        df[f"{col}_z"] = df.groupby("date")[col].transform(zscore)

    # Composite alpha:
    # - positive momentum is good
    # - lower volatility is good
    # - higher liquidity is good
    alpha = (
        0.30 * df["mom_1m_z"] +
        0.25 * df["mom_3m_z"] +
        0.20 * df["mom_6m_z"] +
        0.10 * df["mom_12m_z"] +
        0.10 * (-df["vol_20d_z"]) +
        0.05 * df["liq_log_dv_z"]
    )

    # Clip extremes
    alpha = alpha.clip(lower=-3.0, upper=3.0)

    out = df[["date", "ticker"]].copy()
    out["expected_return"] = alpha

    # Drop very early rows where momentum/vol are NaN-heavy
    out = out.dropna(subset=["expected_return"])

    return out


def main():
    print("Building price-based composite alpha from ingestion_5years.csv...")
    df_alpha = build_alpha()
    df_alpha.to_csv(OUT_FILE, index=False)
    print(f"Saved expected returns to: {OUT_FILE}")


if __name__ == "__main__":
    main()