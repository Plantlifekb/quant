r"""
Quant v1.0 — regime_classifier_quant_v1.py
Version: v1.1 (timezone + dtype fix)

Fixes in v1.1:
- Normalise timezone for both returns and backtest dates (UTC).
- Enforce numeric dtype for portfolio_ret after merge.
- Protect against duplicate dates.
- Rolling windows now compute correctly.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("regime_classifier_quant_v1")

RETURNS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_returns_panel.csv"
BACKTEST_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_backtest_pnl_v1.csv"
OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_regime_states_v1.csv"

TRADING_DAYS = 252
VOL_WINDOW_SHORT = 20
VOL_WINDOW_LONG = 60
DRAWDOWN_WINDOW = 60


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def load_returns() -> pd.DataFrame:
    logger.info("Loading returns panel from %s", RETURNS_FILE)
    df = pd.read_csv(RETURNS_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "ticker", "daily_return"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Returns panel missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    df["daily_return"] = pd.to_numeric(df["daily_return"], errors="coerce").fillna(0.0)

    # Aggregate to portfolio-level daily return
    daily = (
        df.groupby("date", as_index=False)["daily_return"]
        .mean()
        .rename(columns={"daily_return": "portfolio_ret"})
    )

    # Ensure no duplicate dates
    daily = daily.drop_duplicates(subset=["date"]).reset_index(drop=True)

    logger.info("Loaded %d daily return observations.", len(daily))
    return daily


def load_backtest() -> pd.DataFrame:
    logger.info("Loading backtest PnL from %s", BACKTEST_FILE)
    df = pd.read_csv(BACKTEST_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "pnl", "cum_pnl"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Backtest PnL missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.drop_duplicates(subset=["date"]).reset_index(drop=True)

    logger.info("Loaded %d backtest PnL observations.", len(df))
    return df


def compute_regime_features(daily_ret: pd.DataFrame, bt: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing regime features (volatility and drawdown).")

    df = daily_ret.merge(bt[["date", "cum_pnl"]], on="date", how="inner")

    if df.empty:
        raise ValueError("No overlapping dates between returns panel and backtest PnL.")

    # Enforce numeric dtype after merge
    df["portfolio_ret"] = pd.to_numeric(df["portfolio_ret"], errors="coerce").fillna(0.0)

    df = df.sort_values("date").reset_index(drop=True)

    # Realised volatility (annualised)
    df["realised_vol_20d"] = (
        df["portfolio_ret"].rolling(VOL_WINDOW_SHORT).std(ddof=1)
        * np.sqrt(TRADING_DAYS)
    )
    df["realised_vol_60d"] = (
        df["portfolio_ret"].rolling(VOL_WINDOW_LONG).std(ddof=1)
        * np.sqrt(TRADING_DAYS)
    )

    # Rolling drawdown
    roll_max = df["cum_pnl"].rolling(DRAWDOWN_WINDOW, min_periods=1).max()
    df["drawdown_60d"] = df["cum_pnl"] - roll_max

    return df


def classify_regime(row: pd.Series) -> str:
    vol_short = row["realised_vol_20d"]
    vol_long = row["realised_vol_60d"]
    dd = row["drawdown_60d"]

    if np.isnan(vol_short) or np.isnan(vol_long):
        return "unknown"

    high_vol = vol_short > 0.25
    low_vol = vol_short < 0.10
    deep_dd = dd < -0.05 * abs(row["cum_pnl"] + 1e-12)

    if high_vol and deep_dd:
        return "crisis"
    if high_vol:
        return "high_vol"
    if low_vol and row["portfolio_ret"] > 0:
        return "calm_trending_up"
    if low_vol and row["portfolio_ret"] < 0:
        return "calm_trending_down"
    if vol_short > vol_long and row["portfolio_ret"] < 0:
        return "stress_building"
    if vol_short < vol_long and row["portfolio_ret"] > 0:
        return "recovery"

    return "normal"


def build_regime_states() -> pd.DataFrame:
    daily_ret = load_returns()
    bt = load_backtest()
    df = compute_regime_features(daily_ret, bt)

    logger.info("Classifying regimes.")
    df["regime_label"] = df.apply(classify_regime, axis=1)

    df = df[
        [
            "date",
            "realised_vol_20d",
            "realised_vol_60d",
            "drawdown_60d",
            "regime_label",
        ]
    ].copy()

    df = df.sort_values("date").reset_index(drop=True)
    df.columns = [c.lower() for c in df.columns]

    logger.info("Built regime states for %d dates.", len(df))
    return df


def save_regime_states(df: pd.DataFrame, regime_run_date: str) -> None:
    logger.info("Saving regime states to %s", OUT_FILE)
    out = df.copy()
    out["regime_run_date"] = regime_run_date
    out.to_csv(OUT_FILE, index=False, encoding="utf-8")


def main() -> None:
    logger.info("Starting regime_classifier_quant_v1 run (v1.1).")
    regime_run_date = iso_now()

    regimes = build_regime_states()
    save_regime_states(regimes, regime_run_date)

    logger.info("regime_classifier_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()