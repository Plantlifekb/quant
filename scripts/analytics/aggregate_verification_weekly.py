#!/usr/bin/env python3
"""
Aggregate verification: weekly predicted vs realized for picks.
Canonical version — does NOT require week_start in input files.
"""

import os
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path.cwd()
OUT = ROOT / "outputs" / "verification"
OUT.mkdir(parents=True, exist_ok=True)

# Canonical inputs
picks_lo = ROOT / "data" / "signals" / "weekly_selection_canonical.csv"
picks_ls = ROOT / "data" / "signals" / "weekly_selection_longshort_canonical.csv"
expected = ROOT / "data" / "analytics" / "quant_expected_returns_timeseries.csv"
prices = ROOT / "data" / "analytics" / "quant_prices_v1.csv"


# ----------------------------------------------------------------------
# LOAD INPUTS — canonical weekly selection uses `date` as week start
# ----------------------------------------------------------------------
def load():
    pk_lo = pd.read_csv(picks_lo, parse_dates=["date"])
    pk_ls = pd.read_csv(picks_ls, parse_dates=["date"])

    # Rename canonical `date` → internal `week_start`
    pk_lo = pk_lo.rename(columns={"date": "week_start"})
    pk_ls = pk_ls.rename(columns={"date": "week_start"})

    exp = pd.read_csv(expected, parse_dates=["date"])
    pr = pd.read_csv(prices, parse_dates=["date"])

    return pk_lo, pk_ls, exp, pr


# ----------------------------------------------------------------------
# WEEKLY PREDICTED RETURNS
# ----------------------------------------------------------------------
def weekly_predicted(exp):
    exp = exp.rename(columns={"date": "trade_date"})
    exp["trade_date"] = pd.to_datetime(exp["trade_date"])

    # Normalize expected return column
    if "expected_return" not in exp.columns:
        for alt in ["exp_return", "pred", "expected"]:
            if alt in exp.columns:
                exp = exp.rename(columns={alt: "expected_return"})
                break

    exp["one_plus"] = 1 + exp["expected_return"].fillna(0)

    # Derive week_start internally
    exp["week_start"] = (
        exp["trade_date"]
        .dt.to_period("W-MON")
        .apply(lambda r: r.start_time.date())
    )

    agg = exp.groupby(["week_start", "ticker"])["one_plus"].prod().reset_index()
    agg["predicted_week_gain"] = agg["one_plus"] - 1
    agg["week_start"] = pd.to_datetime(agg["week_start"])

    return agg[["week_start", "ticker", "predicted_week_gain"]]


# ----------------------------------------------------------------------
# WEEKLY REALIZED RETURNS
# ----------------------------------------------------------------------
def weekly_realized(pr):
    pr = pr.rename(columns={"date": "trade_date"})
    pr["trade_date"] = pd.to_datetime(pr["trade_date"])
    pr = pr.sort_values(["ticker", "trade_date"])

    pr["week_start"] = (
        pr["trade_date"]
        .dt.to_period("W-MON")
        .apply(lambda r: r.start_time.date())
    )

    grp = pr.groupby(["week_start", "ticker"])

    first = grp.first().reset_index().rename(columns={"close": "close_start"})
    last = grp.last().reset_index().rename(columns={"close": "close_end"})

    merged = first.merge(last, on=["week_start", "ticker"], how="outer")
    merged["realized_week_gain"] = merged["close_end"] / merged["close_start"] - 1
    merged["week_start"] = pd.to_datetime(merged["week_start"])

    return merged[["week_start", "ticker", "close_start", "close_end", "realized_week_gain"]]


# ----------------------------------------------------------------------
# MERGE PREDICTED + REALIZED INTO PICK-LEVEL TABLE
# ----------------------------------------------------------------------
def build_pick_level(pk, pred_df, real_df):
    df = pk.copy()

    df = df.merge(pred_df, on=["week_start", "ticker"], how="left")
    df = df.merge(real_df, on=["week_start", "ticker"], how="left")

    df["tradeable_flag"] = (
        (~df["realized_week_gain"].isna())
        & (~df["close_start"].isna())
        & (~df["close_end"].isna())
    )

    df["predicted_week_gain"] = df["predicted_week_gain"].fillna(0.0)

    return df


# ----------------------------------------------------------------------
# PORTFOLIO-LEVEL WEEKLY AGGREGATION
# ----------------------------------------------------------------------
def portfolio_weekly(pick_df, strategy_name):
    df = pick_df.copy()

    # If no target_weight column, equal weight
    if "target_weight" not in df.columns or df["target_weight"].isna().all():
        df["target_weight"] = (
            df.groupby(["week_start"])["ticker"]
            .transform(lambda x: 1.0 / len(x))
        )

    df["pred_contrib"] = df["target_weight"] * df["predicted_week_gain"]
    df["real_contrib"] = df["target_weight"] * df["realized_week_gain"].fillna(0.0)

    agg = df.groupby("week_start").agg(
        predicted_portfolio_gain=("pred_contrib", "sum"),
        realized_portfolio_gain=("real_contrib", "sum"),
        n_untradeable=("tradeable_flag", lambda s: int((~s).sum())),
    ).reset_index()

    agg["strategy"] = strategy_name
    return agg


# ----------------------------------------------------------------------
# SUMMARY METRICS
# ----------------------------------------------------------------------
def summary_metrics(agg_df):
    out = []

    for strat, g in agg_df.groupby("strategy"):
        pred = g["predicted_portfolio_gain"].fillna(0)
        real = g["realized_portfolio_gain"].fillna(0)
        diff = real - pred

        out.append({
            "strategy": strat,
            "bias": diff.mean(),
            "rmse": np.sqrt((diff ** 2).mean()),
            "hit_rate": (np.sign(pred) == np.sign(real)).mean(),
            "correlation": pred.corr(real) if not np.isnan(pred.corr(real)) else 0.0,
            "coverage": 1 - (g["n_untradeable"].sum() / (len(g) * 10)),
        })

    return pd.DataFrame(out)


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------
if __name__ == "__main__":
    pk_lo, pk_ls, exp, pr = load()

    pred_df = weekly_predicted(exp)
    real_df = weekly_realized(pr)

    picks_lo = build_pick_level(pk_lo, pred_df, real_df)
    picks_ls = build_pick_level(pk_ls, pred_df, real_df)

    picks_lo.to_csv(OUT / "predicted_vs_picks_weekly_longonly.csv", index=False)
    picks_lo.to_csv(OUT / "realized_vs_picks_weekly_longonly.csv", index=False)
    picks_ls.to_csv(OUT / "predicted_vs_picks_weekly_longshort.csv", index=False)
    picks_ls.to_csv(OUT / "realized_vs_picks_weekly_longshort.csv", index=False)

    agg_lo = portfolio_weekly(picks_lo, "longonly")
    agg_ls = portfolio_weekly(picks_ls, "longshort")

    agg = pd.concat([agg_lo, agg_ls], ignore_index=True).sort_values(
        ["strategy", "week_start"]
    )
    agg.to_csv(OUT / "weekly_portfolio_predicted_vs_realized.csv", index=False)

    summary = summary_metrics(agg)
    summary.to_csv(OUT / "verification_summary.csv", index=False)

    print("Wrote verification outputs to:", OUT)