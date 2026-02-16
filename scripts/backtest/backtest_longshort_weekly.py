"""
Quant v1.1 — Long–Short Weekly Backtest

Inputs:
    C:\Quant\data\signals\weekly_selection.csv
        (10‑stock long‑only selection)

    C:\Quant\data\signals\weekly_selection_longshort.csv
        (20‑stock long–short selection: date, ticker, rank, side, weight)

    C:\Quant\data\ingestion\ingestion_5years.csv
        Columns (at least): date, ticker, close

Output:
    C:\Quant\data\analytics\quant_weekly_longshort_perf_v1.csv
        week_start, strategy, weekly_return, cumulative_return, drawdown
"""

import pandas as pd
import os

SEL_LONGONLY_PATH   = r"C:\Quant\data\signals\weekly_selection.csv"
SEL_LONGSHORT_PATH  = r"C:\Quant\data\signals\weekly_selection_longshort.csv"
INGESTION_PATH      = r"C:\Quant\data\ingestion\ingestion_5years.csv"
OUTPUT_PATH         = r"C:\Quant\data\analytics\quant_weekly_longshort_perf_v1.csv"


def load_data():
    lo = pd.read_csv(SEL_LONGONLY_PATH)
    ls = pd.read_csv(SEL_LONGSHORT_PATH)
    px = pd.read_csv(INGESTION_PATH)

    lo["date"] = pd.to_datetime(lo["date"]).dt.date
    ls["date"] = pd.to_datetime(ls["date"]).dt.date
    px["date"] = pd.to_datetime(px["date"]).dt.date

    return lo, ls, px


def build_backtest(lo, ls, px):
    calendar = sorted(px["date"].unique())
    records = []

    # LONG ONLY
    for monday, group in lo.groupby("date"):
        tickers = group["ticker"].tolist()
        if len(tickers) != 10:
            continue

        if monday not in calendar:
            continue
        idx = calendar.index(monday)
        exit_idx = idx + 5
        if exit_idx >= len(calendar):
            continue
        exit_date = calendar[exit_idx]

        monday_px = px[px["date"] == monday].set_index("ticker")["close"]
        exit_px   = px[px["date"] == exit_date].set_index("ticker")["close"]

        if not set(tickers).issubset(monday_px.index) or not set(tickers).issubset(exit_px.index):
            continue

        returns = (exit_px[tickers] - monday_px[tickers]) / monday_px[tickers]
        r = returns.mean()

        records.append({
            "week_start": monday,
            "strategy": "long_only",
            "weekly_return": r
        })

    # LONG SHORT
    for monday, group in ls.groupby("date"):
        longs  = group[group["side"] == "long"]["ticker"].tolist()
        shorts = group[group["side"] == "short"]["ticker"].tolist()

        if len(longs) != 10 or len(shorts) != 10:
            continue

        if monday not in calendar:
            continue
        idx = calendar.index(monday)
        exit_idx = idx + 5
        if exit_idx >= len(calendar):
            continue
        exit_date = calendar[exit_idx]

        monday_px = px[px["date"] == monday].set_index("ticker")["close"]
        exit_px   = px[px["date"] == exit_date].set_index("ticker")["close"]

        if not set(longs).issubset(monday_px.index) or not set(longs).issubset(exit_px.index):
            continue
        if not set(shorts).issubset(monday_px.index) or not set(shorts).issubset(exit_px.index):
            continue

        long_ret  = (exit_px[longs]  - monday_px[longs])  / monday_px[longs]
        short_ret = -(exit_px[shorts] - monday_px[shorts]) / monday_px[shorts]

        r = pd.concat([long_ret, short_ret]).mean()

        records.append({
            "week_start": monday,
            "strategy": "long_short",
            "weekly_return": r
        })

    df = pd.DataFrame(records).sort_values(["strategy", "week_start"]).reset_index(drop=True)

    out = []
    for strat, g in df.groupby("strategy"):
        g = g.copy()
        g["cumulative_return"] = (1 + g["weekly_return"]).cumprod() - 1
        g["drawdown"] = (g["cumulative_return"] - g["cumulative_return"].cummax()) / g["cumulative_return"].cummax()
        g["drawdown"] = g["drawdown"].fillna(0)
        out.append(g)

    return pd.concat(out, ignore_index=True)


def main():
    lo, ls, px = load_data()
    result = build_backtest(lo, ls, px)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)
    print(f"Long‑only + long‑short backtest written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()