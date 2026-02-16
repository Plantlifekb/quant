"""
Quant v1.1 — Verification: Long‑Only Backtest vs Ingestion (Model‑Aligned)

Compares:
    1) Weekly returns recomputed from ingestion_5years.csv
       using the *actual* weekly_selection.csv (model output)

    2) Weekly returns from quant_weekly_longonly_perf_v1.csv
       (backtest_longonly_weekly.py output)
"""

import pandas as pd
import numpy as np
import os

INGESTION_PATH   = r"C:\Quant\data\ingestion\ingestion_5years.csv"
SELECTION_PATH   = r"C:\Quant\data\signals\weekly_selection.csv"
BACKTEST_PATH    = r"C:\Quant\data\analytics\quant_weekly_longonly_perf_v1.csv"


def recompute_weekly_from_ingestion(px, sel):
    """
    For each Monday in weekly_selection.csv:
        - take the 10 selected tickers
        - compute 5‑day forward return from ingestion
        - average them
    """
    px = px.copy()
    px["date"] = pd.to_datetime(px["date"]).dt.date
    sel = sel.copy()
    sel["date"] = pd.to_datetime(sel["date"]).dt.date

    calendar = sorted(px["date"].unique())
    records = []

    for monday, group in sel.groupby("date"):
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

        rets = (exit_px[tickers] - monday_px[tickers]) / monday_px[tickers]
        r = rets.mean()

        records.append({
            "week_start": monday,
            "weekly_return_calc": r
        })

    return pd.DataFrame(records).sort_values("week_start").reset_index(drop=True)


def main():
    print("Loading data...")
    px  = pd.read_csv(INGESTION_PATH)
    sel = pd.read_csv(SELECTION_PATH)
    bt  = pd.read_csv(BACKTEST_PATH)

    bt["week_start"] = pd.to_datetime(bt["week_start"]).dt.date
    bt_longonly = bt[bt["strategy"] == "long_only"].copy()

    print("Recomputing long-only weekly returns from ingestion data (model-aligned)...")
    calc = recompute_weekly_from_ingestion(px, sel)

    merged = pd.merge(
        calc,
        bt_longonly[["week_start", "weekly_return"]],
        on="week_start",
        how="inner"
    )

    merged["weekly_diff"] = merged["weekly_return_calc"] - merged["weekly_return"]
    merged["weekly_pass"] = np.isclose(merged["weekly_diff"], 0.0, atol=1e-8)

    print("============================================================")
    print("QUANT v1.1 VERIFICATION REPORT (LONG ONLY, MODEL-ALIGNED)")
    print("============================================================")
    print(merged)
    print("============================================================")

    if merged["weekly_pass"].all():
        print("ALL WEEKS PASS — BACKTEST MATCHES INGESTION FOR LONG-ONLY.")
    else:
        print("FAILURES DETECTED — SEE ABOVE.")


if __name__ == "__main__":
    main()