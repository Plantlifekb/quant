"""
Quant v1.2 — Combined Analytics Builder

Inputs:
    C:\Quant\data\analytics\quant_weekly_longonly_perf_v1.csv
        week_start, strategy=long_only, weekly_return, cumulative_return, drawdown

    C:\Quant\data\analytics\quant_weekly_longshort_perf_v1.csv
        week_start, strategy in {long_only, long_short}, weekly_return, cumulative_return, drawdown

Output:
    C:\Quant\data\analytics\quant_combined_analytics_v1.csv
        week_start,
        lo_weekly_return, ls_weekly_return, spread_weekly_return,
        lo_cum_return,   ls_cum_return,   spread_cum_return,
        lo_drawdown,     ls_drawdown,     spread_drawdown
"""

import pandas as pd
import numpy as np
import os

LO_PATH   = r"C:\Quant\data\analytics\quant_weekly_longonly_perf_v1.csv"
LS_PATH   = r"C:\Quant\data\analytics\quant_weekly_longshort_perf_v1.csv"
OUT_PATH  = r"C:\Quant\data\analytics\quant_combined_analytics_v1.csv"


def main():
    lo = pd.read_csv(LO_PATH)
    ls = pd.read_csv(LS_PATH)

    lo["week_start"] = pd.to_datetime(lo["week_start"])
    ls["week_start"] = pd.to_datetime(ls["week_start"])

    lo_only = lo[lo["strategy"] == "long_only"].copy()
    ls_lo   = ls[ls["strategy"] == "long_only"].copy()
    ls_ls   = ls[ls["strategy"] == "long_short"].copy()

    # sanity: long_only from both files should match
    lo_merged = pd.merge(
        lo_only[["week_start", "weekly_return"]],
        ls_lo[["week_start", "weekly_return"]],
        on="week_start",
        suffixes=("_lo_file", "_ls_file"),
        how="inner"
    )
    if not np.isclose(
        lo_merged["weekly_return_lo_file"],
        lo_merged["weekly_return_ls_file"],
        atol=1e-8
    ).all():
        print("WARNING: long_only series differ between files.")

    # base frame: all weeks where we have both long_only and long_short
    base = pd.merge(
        lo_only[["week_start", "weekly_return", "cumulative_return", "drawdown"]],
        ls_ls[["week_start", "weekly_return", "cumulative_return", "drawdown"]],
        on="week_start",
        suffixes=("_lo", "_ls"),
        how="inner"
    ).sort_values("week_start").reset_index(drop=True)

    base.rename(columns={
        "weekly_return_lo": "lo_weekly_return",
        "weekly_return_ls": "ls_weekly_return",
        "cumulative_return_lo": "lo_cum_return",
        "cumulative_return_ls": "ls_cum_return",
        "drawdown_lo": "lo_drawdown",
        "drawdown_ls": "ls_drawdown",
    }, inplace=True)

    # spread = long_short - long_only
    base["spread_weekly_return"] = base["ls_weekly_return"] - base["lo_weekly_return"]

    base["spread_cum_return"] = (1 + base["spread_weekly_return"]).cumprod() - 1
    base["spread_drawdown"] = (
        base["spread_cum_return"] - base["spread_cum_return"].cummax()
    ) / base["spread_cum_return"].cummax()
    base["spread_drawdown"] = base["spread_drawdown"].fillna(0)

    out_cols = [
        "week_start",
        "lo_weekly_return", "ls_weekly_return", "spread_weekly_return",
        "lo_cum_return",   "ls_cum_return",   "spread_cum_return",
        "lo_drawdown",     "ls_drawdown",     "spread_drawdown",
    ]
    base = base[out_cols]

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    base.to_csv(OUT_PATH, index=False)
    print(f"Combined analytics written to: {OUT_PATH}")


if __name__ == "__main__":
    main()