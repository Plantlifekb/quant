"""
Quant v1.1 — Long‑Only Weekly Backtest (Baseline Validation)

Inputs:
    C:\Quant\data\signals\weekly_selection.csv
        Columns: date, ticker, score, weight   (10 tickers per week)

    C:\Quant\data\ingestion\ingestion_5years.csv
        Columns (at least): date, ticker, close

Output:
    C:\Quant\data\analytics\quant_weekly_longonly_perf_v1.csv
        week_start, strategy, weekly_return, cumulative_return, drawdown
"""

import pandas as pd
import os

SEL_PATH      = r"C:\Quant\data\signals\weekly_selection.csv"
INGESTION_PATH = r"C:\Quant\data\ingestion\ingestion_5years.csv"
OUTPUT_PATH    = r"C:\Quant\data\analytics\quant_weekly_longonly_perf_v1.csv"


def load_data():
    sel = pd.read_csv(SEL_PATH)
    px = pd.read_csv(INGESTION_PATH)

    sel["date"] = pd.to_datetime(sel["date"]).dt.date
    px["date"]  = pd.to_datetime(px["date"]).dt.date

    return sel, px


def compute_weekly_return(tickers, monday, calendar, px):
    if monday not in calendar:
        return None

    idx = calendar.index(monday)
    exit_idx = idx + 5
    if exit_idx >= len(calendar):
        return None

    exit_date = calendar[exit_idx]

    monday_px = px[px["date"] == monday].set_index("ticker")["close"]
    exit_px   = px[px["date"] == exit_date].set_index("ticker")["close"]

    if not set(tickers).issubset(monday_px.index) or not set(tickers).issubset(exit_px.index):
        return None

    returns = (exit_px[tickers] - monday_px[tickers]) / monday_px[tickers]
    return returns.mean()


def build_backtest(sel, px):
    calendar = sorted(px["date"].unique())
    records = []

    for monday, group in sel.groupby("date"):
        tickers = group["ticker"].tolist()
        if len(tickers) != 10:
            continue

        r = compute_weekly_return(tickers, monday, calendar, px)
        if r is None:
            continue

        records.append({
            "week_start": monday,
            "strategy": "long_only",
            "weekly_return": r
        })

    df = pd.DataFrame(records).sort_values("week_start").reset_index(drop=True)

    df["cumulative_return"] = (1 + df["weekly_return"]).cumprod() - 1
    df["drawdown"] = (df["cumulative_return"] - df["cumulative_return"].cummax()) / df["cumulative_return"].cummax()
    df["drawdown"] = df["drawdown"].fillna(0)

    return df


def main():
    sel, px = load_data()
    result = build_backtest(sel, px)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)
    print(f"Long‑only backtest written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()