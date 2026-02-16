#!/usr/bin/env python3
# ==================================================================================================
# Quant v1.0 â€” Performance & Risk Engine (Governed)
# ==================================================================================================
# PURPOSE:
#   Compute portfolio-level performance and risk metrics from the backtest dataset.
#
# INPUT:
#   C:\Quant\data\analytics\quant_backtest.csv
#
# OUTPUT:
#   C:\Quant\data\analytics\quant_performance.csv
#   C:\Quant\data\analytics\quant_summary.txt
#
# GOVERNANCE:
#   â€¢ Deterministic
#   â€¢ No drift
#   â€¢ Full provenance
#   â€¢ No overwriting upstream layers
# ==================================================================================================

import os
import pandas as pd
import numpy as np
from datetime import datetime

import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from logging_quant_v1 import get_logger

# ==================================================================================================
# PATHS
# ==================================================================================================

ROOT = r"C:\Quant"
BACKTEST = os.path.join(ROOT, "data", "analytics", "quant_backtest.csv")
OUT_DAILY = os.path.join(ROOT, "data", "analytics", "quant_performance.csv")
OUT_SUMMARY = os.path.join(ROOT, "data", "analytics", "quant_summary.txt")

# ==================================================================================================
# METRIC FUNCTIONS
# ==================================================================================================

def max_drawdown(series):
    cummax = series.cummax()
    dd = (series - cummax) / cummax
    return dd.min()

def annualized_return(daily_ret):
    return (1 + daily_ret.mean()) ** 252 - 1

def annualized_vol(daily_ret):
    return daily_ret.std() * np.sqrt(252)

def sharpe(daily_ret):
    vol = annualized_vol(daily_ret)
    return np.nan if vol == 0 else annualized_return(daily_ret) / vol

def sortino(daily_ret):
    downside = daily_ret[daily_ret < 0].std() * np.sqrt(252)
    return np.nan if downside == 0 else annualized_return(daily_ret) / downside

# ==================================================================================================
# MAIN
# ==================================================================================================

def run():
    logger = get_logger("performance_engine_quant_v1")
    logger.info("=== Starting performance engine ===")

    if not os.path.exists(BACKTEST):
        raise FileNotFoundError("Backtest dataset missing.")

    df = pd.read_csv(BACKTEST)
    df.columns = [c.lower() for c in df.columns]

    # Daily portfolio returns
    daily = df.groupby("date")["portfolio_ret"].first().reset_index()
    daily["cum_ret"] = (1 + daily["portfolio_ret"]).cumprod()

    # Rolling metrics
    daily["roll_vol_20"] = daily["portfolio_ret"].rolling(20).std() * np.sqrt(252)
    daily["roll_sharpe_20"] = (
        daily["portfolio_ret"].rolling(20).mean() /
        daily["portfolio_ret"].rolling(20).std()
    ) * np.sqrt(252)

    # Drawdown
    daily["drawdown"] = daily["cum_ret"] / daily["cum_ret"].cummax() - 1

    # Provenance
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    daily["performance_run_date"] = run_date

    # Save daily metrics
    daily.to_csv(OUT_DAILY, index=False, encoding="utf-8")
    logger.info(f"Daily performance written: {OUT_DAILY}")

    # Summary metrics
    ar = annualized_return(daily["portfolio_ret"])
    av = annualized_vol(daily["portfolio_ret"])
    sr = sharpe(daily["portfolio_ret"])
    so = sortino(daily["portfolio_ret"])
    mdd = max_drawdown(daily["cum_ret"])
    cr = np.nan if mdd == 0 else ar / abs(mdd)

    summary = f"""
Quant v1.0 â€” Performance Summary
Run date: {run_date}

Annualized Return:     {ar:.4f}
Annualized Volatility: {av:.4f}
Sharpe Ratio:          {sr:.4f}
Sortino Ratio:         {so:.4f}
Max Drawdown:          {mdd:.4f}
Calmar Ratio:          {cr:.4f}
"""

    with open(OUT_SUMMARY, "w", encoding="utf-8") as f:
        f.write(summary)

    logger.info(f"Summary written: {OUT_SUMMARY}")
    logger.info("Done.")

    return daily

if __name__ == "__main__":
    run()

