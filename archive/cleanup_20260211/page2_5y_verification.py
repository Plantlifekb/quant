"""
page2_5y_weekly_strategy.py
Quant v1.0

Purpose:
    Simulate 5 years of weekly performance by:
    - Each week taking that week's long-only tradable weights
    - Buying on that date
    - Holding for one week
    - Recording the portfolio's weekly return

Inputs:
    C:\Quant\data\analytics\quant_portfolio_weights_tradable_v1_osqp.csv
    C:\Quant\data\ingestion\ingestion_5years.csv
"""

import pandas as pd
import numpy as np
import streamlit as st
from pathlib import Path

BASE = Path(r"C:\Quant\data")
WEIGHTS = BASE / "analytics" / "quant_portfolio_weights_tradable_v1_osqp.csv"
INGEST = BASE / "ingestion" / "ingestion_5years.csv"

st.set_page_config(page_title="5-Year Weekly Strategy", layout="wide")
st.title("5-Year Weekly Strategy Performance (Long Only)")

# --- Load weights ---
w = pd.read_csv(WEIGHTS)
w.columns = [c.lower() for c in w.columns]

required = {"date", "ticker", "weight_tradable_v1"}
missing = required - set(w.columns)
if missing:
    st.error(f"Missing columns in weights file: {', '.join(sorted(missing))}")
    st.stop()

w["date"] = pd.to_datetime(w["date"])
w["ticker"] = w["ticker"].astype(str).str.upper()
w["weight_tradable_v1"] = pd.to_numeric(w["weight_tradable_v1"], errors="coerce")

# Long-only: drop negative weights
w = w[w["weight_tradable_v1"] > 0].copy()
if w.empty:
    st.error("No positive (long) weights in weights file.")
    st.stop()

rebalance_dates = sorted(w["date"].unique())
if len(rebalance_dates) < 2:
    st.error("Not enough rebalance dates to compute weekly strategy.")
    st.stop()

top_n = st.sidebar.slider("Number of picks per week", 5, 50, 20)

# --- Load prices ---
ing = pd.read_csv(INGEST)
ing.columns = [c.lower() for c in ing.columns]

required_ing = {"date", "ticker", "adj_close"}
missing_ing = required_ing - set(ing.columns)
if missing_ing:
    st.error(f"Missing columns in ingestion file: {', '.join(sorted(missing_ing))}")
    st.stop()

ing["date"] = pd.to_datetime(ing["date"])
ing["ticker"] = ing["ticker"].astype(str).str.upper()

price_pivot = ing.pivot(index="date", columns="ticker", values="adj_close").sort_index()

weekly_records = []

for i in range(len(rebalance_dates) - 1):
    d0 = rebalance_dates[i]
    d1 = rebalance_dates[i + 1]  # next rebalance date ~ one week later

    week_weights = w[w["date"] == d0].copy()
    if week_weights.empty:
        continue

    week_weights["abs_weight"] = week_weights["weight_tradable_v1"].abs()
    week_weights = week_weights.sort_values("abs_weight", ascending=False).head(top_n)

    tickers = week_weights["ticker"].unique()

    # Prices at start and end of week
    if d0 not in price_pivot.index or d1 not in price_pivot.index:
        continue

    p0 = price_pivot.loc[d0, tickers]
    p1 = price_pivot.loc[d1, tickers]

    df = pd.DataFrame(
        {"ticker": tickers, "p0": p0.values, "p1": p1.values}
    ).set_index("ticker")

    df["ret"] = (df["p1"] / df["p0"]) - 1.0

    ww = week_weights.set_index("ticker")["weight_tradable_v1"]
    df = df.join(ww, how="inner")

    if df.empty:
        continue

    # Normalise weights to sum to 1 (long-only capital fully deployed)
    total_w = df["weight_tradable_v1"].sum()
    if total_w <= 0:
        continue
    df["norm_w"] = df["weight_tradable_v1"] / total_w

    port_ret = float((df["norm_w"] * df["ret"]).sum())

    weekly_records.append(
        {
            "start_date": d0,
            "end_date": d1,
            "weekly_return": port_ret,
        }
    )

if not weekly_records:
    st.error("No valid weekly periods could be constructed from weights and prices.")
    st.stop()

weekly = pd.DataFrame(weekly_records).sort_values("start_date")
weekly["cumulative"] = (1 + weekly["weekly_return"]).cumprod() - 1

def pct(x):
    return f"{x*100:.1f}%"

st.subheader("Weekly Returns (Strategy)")
st.line_chart(weekly.set_index("end_date")["weekly_return"])

st.subheader("Cumulative Growth (Strategy)")
st.line_chart(weekly.set_index("end_date")["cumulative"])

st.subheader("Summary Metrics")
total_return = weekly["cumulative"].iloc[-1]
avg_weekly = weekly["weekly_return"].mean()
annualised = (1 + avg_weekly) ** 52 - 1

summary = pd.DataFrame(
    {
        "Metric": [
            "Total return (5y)",
            "Avg weekly return",
            "Annualised return",
        ],
        "Value": [
            pct(total_return),
            pct(avg_weekly),
            pct(annualised),
        ],
    }
)

st.table(summary)