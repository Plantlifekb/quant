import pandas as pd
import numpy as np

INGESTION_PATH  = r"C:\Quant\data\ingestion\ingestion_5years.csv"
SELECTION_PATH  = r"C:\Quant\data\signals\weekly_selection_longshort.csv"
BACKTEST_PATH   = r"C:\Quant\data\analytics\quant_weekly_longshort_perf_v1.csv"


def recompute_longshort(px, sel):
    px = px.copy()
    px["date"] = pd.to_datetime(px["date"]).dt.date
    sel = sel.copy()
    sel["date"] = pd.to_datetime(sel["date"]).dt.date

    calendar = sorted(px["date"].unique())
    records = []

    for monday, g in sel.groupby("date"):
        longs  = g[g["side"] == "long"]["ticker"].tolist()
        shorts = g[g["side"] == "short"]["ticker"].tolist()

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
            "weekly_return_calc": r
        })

    return pd.DataFrame(records).sort_values("week_start").reset_index(drop=True)


def main():
    print("Loading data...")
    px  = pd.read_csv(INGESTION_PATH)
    sel = pd.read_csv(SELECTION_PATH)
    bt  = pd.read_csv(BACKTEST_PATH)

    bt["week_start"] = pd.to_datetime(bt["week_start"]).dt.date
    bt_ls = bt[bt["strategy"] == "long_short"].copy()

    print("Recomputing long‑short weekly returns from ingestion...")
    calc = recompute_longshort(px, sel)

    merged = pd.merge(
        calc,
        bt_ls[["week_start", "weekly_return"]],
        on="week_start",
        how="inner"
    )

    merged["weekly_diff"] = merged["weekly_return_calc"] - merged["weekly_return"]
    merged["weekly_pass"] = np.isclose(merged["weekly_diff"], 0.0, atol=1e-8)

    print("============================================================")
    print("QUANT v1.1 VERIFICATION REPORT (LONG‑SHORT)")
    print("============================================================")
    print(merged)
    print("============================================================")

    if merged["weekly_pass"].all():
        print("ALL WEEKS PASS — BACKTEST MATCHES INGESTION FOR LONG‑SHORT.")
    else:
        print("FAILURES DETECTED — SEE ABOVE.")


if __name__ == "__main__":
    main()