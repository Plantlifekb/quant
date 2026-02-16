# =====================================================================
# Module: quant_backtest_v1.py
# Quant Version: v1.0
#
# Purpose:
#   Produce the canonical governed backtest PnL for Quant v1.0.
#   This module is the single source of truth for:
#       - daily pnl
#       - cumulative pnl
#       - gross exposure
#       - net exposure
#       - turnover
#       - transaction costs (if enabled)
#
# Description:
#   - Reads canonical realized returns (quant_realized_returns_v1.csv)
#   - Reads tradable portfolio weights (quant_portfolio_weights_tradable_v1_osqp.csv)
#   - Aligns weights and returns by date/ticker
#   - Applies leverage scaling (configurable)
#   - Computes turnover and transaction costs (configurable)
#   - Computes daily pnl and cumulative pnl
#   - Writes quant_backtest_pnl_v1.csv to governed analytics directory
#
# Inputs:
#   C:\Quant\data\analytics\quant_realized_returns_v1.csv
#       Columns:
#           - date
#           - ticker
#           - return_close_to_close
#
#   C:\Quant\data\analytics\quant_portfolio_weights_tradable_v1_osqp.csv
#       Columns:
#           - date
#           - ticker
#           - weight_tradable_v1
#
#   C:\Quant\config\backtest_config_v1.json
#       Keys:
#           - leverage_target
#           - transaction_cost_bps
#           - turnover_cost_bps
#
# Outputs:
#   C:\Quant\data\analytics\quant_backtest_pnl_v1.csv
#       Columns:
#           - date
#           - pnl
#           - cum_pnl
#           - gross_exposure
#           - net_exposure
#           - turnover
#           - costs
#
# Governance Rules:
#   - No schema drift: output columns must match exactly.
#   - No hidden data: only governed inputs may be used.
#   - Deterministic: same inputs must produce identical outputs.
#   - All dates must be ISO-8601.
#   - All tickers must be uppercase.
#   - All columns must be lowercase.
#
# Provenance:
#   - This module is part of the governed Quant v1.0 pipeline.
#   - Any change requires version bump (e.g., quant_backtest_v1_1.py).
# =====================================================================

import os
import json
import pandas as pd

DATA_DIR = r"C:\Quant\data"
ANALYTICS_DIR = os.path.join(DATA_DIR, "analytics")
CONFIG_DIR = r"C:\Quant\config"

RETURNS_FILE = os.path.join(ANALYTICS_DIR, "quant_realized_returns_v1.csv")
WEIGHTS_FILE = os.path.join(ANALYTICS_DIR, "quant_portfolio_weights_tradable_v1_osqp.csv")
CONFIG_FILE = os.path.join(CONFIG_DIR, "backtest_config_v1.json")

OUT_FILE = os.path.join(ANALYTICS_DIR, "quant_backtest_pnl_v1.csv")


def load_config():
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


def load_returns():
    df = pd.read_csv(RETURNS_FILE)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str).str.upper()
    return df


def load_weights():
    df = pd.read_csv(WEIGHTS_FILE)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df.rename(columns={"weight_tradable_v1": "weight"})
    return df


def compute_turnover(df):
    df = df.sort_values(["ticker", "date"])
    df["prev_weight"] = df.groupby("ticker")["weight"].shift(1)
    df["turnover"] = (df["weight"] - df["prev_weight"]).abs()
    df["turnover"] = df["turnover"].fillna(0)
    return df


def main():
    print("Loading governed inputs...")
    cfg = load_config()
    returns = load_returns()
    weights = load_weights()

    print("Merging weights and returns...")
    df = weights.merge(returns, on=["date", "ticker"], how="inner")

    print("Applying leverage scaling...")
    leverage = cfg.get("leverage_target", 1.0)
    df["weight"] = df["weight"] * leverage

    print("Computing turnover...")
    df = compute_turnover(df)

    print("Computing transaction costs...")
    cost_bps = cfg.get("transaction_cost_bps", 0.0)
    df["costs"] = df["turnover"] * (cost_bps / 10000.0)

    print("Computing daily pnl...")
    df["pnl"] = df["weight"] * df["return_close_to_close"] - df["costs"]

    print("Aggregating to daily level...")
    daily = (
        df.groupby("date", as_index=False)
        .agg(
            pnl=("pnl", "sum"),
            gross_exposure=("weight", lambda x: x.abs().sum()),
            net_exposure=("weight", "sum"),
            turnover=("turnover", "sum"),
            costs=("costs", "sum"),
        )
    )

    daily["cum_pnl"] = daily["pnl"].cumsum()

    print(f"Writing governed backtest PnL to: {OUT_FILE}")
    daily.to_csv(OUT_FILE, index=False, encoding="utf-8")

    print("Backtest generation complete.")


if __name__ == "__main__":
    main()