"""
Module: backtest_10stock_weekly.py
Quant variant: 10-stock weekly strategy

Purpose:
    Compute weekly and cumulative returns for the 10-stock Monday strategy
    using strict 5-trading-day forward returns and strict basket integrity.

Rules:
    - Monday → Monday+5 trading days (strict)
    - If Monday+5 does not exist → skip week
    - If ANY of the 10 tickers is missing the exit price → skip week
    - Equal weight (0.10 each)
    - Deterministic, governed, no schema drift

Inputs:
    C:\Quant\data\signals\weekly_selection.csv
    C:\Quant\data\enriched\enriched_10stock_weekly.csv

Output:
    C:\Quant\data\backtest\backtest_10stock_weekly.csv
"""

import os
import pandas as pd

SELECTION_PATH = r"C:\Quant\data\signals\weekly_selection.csv"
ENRICHED_PATH = r"C:\Quant\data\enriched\enriched_10stock_weekly.csv"

OUTPUT_DIR = r"C:\Quant\data\backtest"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "backtest_10stock_weekly.csv")


def load_data():
    sel = pd.read_csv(SELECTION_PATH)
    enr = pd.read_csv(ENRICHED_PATH)

    sel["date"] = pd.to_datetime(sel["date"])
    enr["date"] = pd.to_datetime(enr["date"])

    # Keep only needed columns
    enr = enr[["date", "ticker", "close"]]

    return sel, enr


def build_trading_calendar(enriched_df):
    calendar = sorted(enriched_df["date"].unique())
    return calendar


def compute_weekly_returns(selection_df, enriched_df, calendar):
    results = []

    # Map date → index for fast lookup
    date_to_idx = {d: i for i, d in enumerate(calendar)}

    for monday, group in selection_df.groupby("date"):

        # Ensure Monday exists in calendar
        if monday not in date_to_idx:
            continue

        idx = date_to_idx[monday]
        exit_idx = idx + 5

        # If Monday+5 is out of range → skip week
        if exit_idx >= len(calendar):
            continue

        exit_date = calendar[exit_idx]

        tickers = list(group["ticker"])

        # Monday prices
        monday_prices = enriched_df[
            (enriched_df["date"] == monday) &
            (enriched_df["ticker"].isin(tickers))
        ].set_index("ticker")["close"]

        # Exit prices
        exit_prices = enriched_df[
            (enriched_df["date"] == exit_date) &
            (enriched_df["ticker"].isin(tickers))
        ].set_index("ticker")["close"]

        # Strict basket integrity: require all 10 tickers
        if len(monday_prices) != 10 or len(exit_prices) != 10:
            continue

        weekly_returns = (exit_prices - monday_prices) / monday_prices
        portfolio_return = weekly_returns.mean()

        results.append({
            "date": monday,
            "portfolio_weekly_return": portfolio_return
        })

    df = pd.DataFrame(results).sort_values("date").reset_index(drop=True)

    if not df.empty:
        df["cumulative_return"] = (1 + df["portfolio_weekly_return"]).cumprod() - 1
    else:
        df["cumulative_return"] = []

    return df


def main():
    if not os.path.exists(SELECTION_PATH):
        raise FileNotFoundError(f"Selection file not found: {SELECTION_PATH}")

    if not os.path.exists(ENRICHED_PATH):
        raise FileNotFoundError(f"Enriched file not found: {ENRICHED_PATH}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    sel, enr = load_data()
    calendar = build_trading_calendar(enr)
    backtest = compute_weekly_returns(sel, enr, calendar)

    backtest.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")


if __name__ == "__main__":
    main()