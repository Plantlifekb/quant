r"""
Quant v1.0 — regime_portfolio_controls_quant_v1.py
Version: v1.0

Purpose
- Convert regime adjustments into concrete portfolio controls:
  - target_volatility (regime-conditioned)
  - max_turnover (regime-conditioned)
- Provide a clean, governed control surface for portfolio_engine_quant_v2.

Inputs
- quant_regime_adjustments_v1.csv
    Columns:
      - date
      - regime_label
      - risk_multiplier
      - turnover_cap

- quant_backtest_pnl_v1.csv (for realised vol)
- quant_returns_panel.csv (for realised vol)

Outputs
- quant_portfolio_controls_v1.csv
    Columns:
      - date
      - target_volatility
      - max_turnover
      - risk_multiplier
      - turnover_cap
      - portfolio_controls_run_date
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("regime_portfolio_controls_quant_v1")

REGIME_ADJ_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_regime_adjustments_v1.csv"
RETURNS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_returns_panel.csv"
BACKTEST_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_backtest_pnl_v1.csv"
OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_controls_v1.csv"

TRADING_DAYS = 252
VOL_WINDOW = 20


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def load_regime_adjustments() -> pd.DataFrame:
    df = pd.read_csv(REGIME_ADJ_FILE)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], utc=True)
    return df.sort_values("date").reset_index(drop=True)


def load_returns() -> pd.DataFrame:
    df = pd.read_csv(RETURNS_FILE)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df["daily_return"] = pd.to_numeric(df["daily_return"], errors="coerce").fillna(0.0)

    daily = (
        df.groupby("date", as_index=False)["daily_return"]
        .mean()
        .rename(columns={"daily_return": "portfolio_ret"})
    )

    daily["portfolio_ret"] = pd.to_numeric(daily["portfolio_ret"], errors="coerce")
    return daily.sort_values("date").reset_index(drop=True)


def compute_realised_vol(daily: pd.DataFrame) -> pd.DataFrame:
    df = daily.copy()
    df["realised_vol_20d"] = (
        df["portfolio_ret"].rolling(VOL_WINDOW).std(ddof=1) * np.sqrt(TRADING_DAYS)
    )
    return df[["date", "realised_vol_20d"]]


def build_controls(regime_adj: pd.DataFrame, realised_vol: pd.DataFrame) -> pd.DataFrame:
    df = regime_adj.merge(realised_vol, on="date", how="left")

    # Baseline target vol (e.g. 10%)
    BASE_TARGET_VOL = 0.10

    df["target_volatility"] = (
        BASE_TARGET_VOL * df["risk_multiplier"]
    ).clip(lower=0.02, upper=0.25)

    df["max_turnover"] = df["turnover_cap"]

    df = df[
        [
            "date",
            "target_volatility",
            "max_turnover",
            "risk_multiplier",
            "turnover_cap",
        ]
    ]

    return df.sort_values("date").reset_index(drop=True)


def save_controls(df: pd.DataFrame, run_date: str) -> None:
    out = df.copy()
    out["portfolio_controls_run_date"] = run_date
    out.to_csv(OUT_FILE, index=False, encoding="utf-8")


def main() -> None:
    logger.info("Starting regime_portfolio_controls_quant_v1 run (v1.0).")
    run_date = iso_now()

    regime_adj = load_regime_adjustments()
    daily = load_returns()
    realised_vol = compute_realised_vol(daily)

    controls = build_controls(regime_adj, realised_vol)
    save_controls(controls, run_date)

    logger.info("regime_portfolio_controls_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()