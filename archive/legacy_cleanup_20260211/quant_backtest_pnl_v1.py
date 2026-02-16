r"""
Quant v1.0 — PnL Backtest Loop (v1.1 governed upgrade)

Enhancements in v1.1:
- Adds backtest_run_date provenance to output.
- Enforces ISO-8601 timezone-aware dates.
- Strengthens schema validation.
- Ensures deterministic, governed behaviour.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

WEIGHTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_tradable_v1_osqp.csv"
RETURNS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_expected_returns_timeseries.csv"
OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_backtest_pnl_v1.csv"


# ---------------------------------------------------------
# Loaders
# ---------------------------------------------------------

def load_weights() -> pd.DataFrame:
    df = pd.read_csv(WEIGHTS_FILE)
    df.columns = [c.lower() for c in df.columns]

    if "date" not in df.columns or "ticker" not in df.columns or "weight_tradable_v1" not in df.columns:
        raise ValueError("Weights file missing required columns: date, ticker, weight_tradable_v1")

    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df.dropna(subset=["date", "ticker", "weight_tradable_v1"])
    return df


def load_returns() -> pd.DataFrame:
    df = pd.read_csv(RETURNS_FILE)
    df.columns = [c.lower() for c in df.columns]

    if "date" not in df.columns or "ticker" not in df.columns:
        raise ValueError("Returns file missing required columns: date, ticker")

    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df["ticker"] = df["ticker"].astype(str).str.upper()

    # expected_return → ret
    if "expected_return" in df.columns:
        df = df.rename(columns={"expected_return": "ret"})
    elif "ret" not in df.columns:
        df["ret"] = 0.0

    df["ret"] = pd.to_numeric(df["ret"], errors="coerce").fillna(0.0)
    return df[["date", "ticker", "ret"]]


# ---------------------------------------------------------
# Core PnL Logic
# ---------------------------------------------------------

def compute_pnl(weights: pd.DataFrame, returns: pd.DataFrame) -> pd.DataFrame:
    df = weights.merge(returns, on=["date", "ticker"], how="left")
    df["ret"] = df["ret"].fillna(0.0)

    df = df.sort_values(["ticker", "date"])
    df["weight_lag"] = df.groupby("ticker")["weight_tradable_v1"].shift(1).fillna(0.0)

    df["pnl"] = df["weight_lag"] * df["ret"]

    daily = df.groupby("date").agg(
        pnl=("pnl", "sum"),
        gross_exposure=("weight_tradable_v1", lambda x: x.abs().sum()),
        net_exposure=("weight_tradable_v1", "sum"),
        turnover=("weight_tradable_v1", lambda x: x.diff().abs().sum()),
    )

    daily["cum_pnl"] = daily["pnl"].cumsum()

    sharpe = daily["pnl"].mean() / (daily["pnl"].std() + 1e-12)
    daily["sharpe"] = sharpe

    roll_max = daily["cum_pnl"].cummax()
    drawdown = daily["cum_pnl"] - roll_max
    daily["drawdown"] = drawdown
    daily["max_drawdown"] = drawdown.min()

    daily = daily.reset_index()
    return daily


# ---------------------------------------------------------
# Save with Provenance
# ---------------------------------------------------------

def save_results(df: pd.DataFrame, backtest_run_date: str) -> None:
    df = df.copy()
    df["backtest_run_date"] = backtest_run_date
    df.to_csv(OUT_FILE, index=False, encoding="utf-8")
    print(f"Saved backtest results to: {OUT_FILE}")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main() -> None:
    print("Loading weights...")
    weights = load_weights()

    print("Loading returns...")
    returns = load_returns()

    print("Computing PnL...")
    pnl = compute_pnl(weights, returns)

    backtest_run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    print("Saving results...")
    save_results(pnl, backtest_run_date)

    print("Backtest complete.")


if __name__ == "__main__":
    main()