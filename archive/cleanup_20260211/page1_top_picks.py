"""
page1_top_picks.py
Quant v1.0

Purpose:
    Show Quant's current top tradable long picks for the latest run date.

Inputs:
    C:\Quant\data\analytics\quant_portfolio_weights_tradable_v1_osqp.csv
    C:\Quant\data\ingestion\ingestion_5years.csv
"""

import pandas as pd
import streamlit as st
from pathlib import Path

BASE = Path(r"C:\Quant\data")
WEIGHTS = BASE / "analytics" / "quant_portfolio_weights_tradable_v1_osqp.csv"
INGEST = BASE / "ingestion" / "ingestion_5years.csv"

st.set_page_config(page_title="Top Picks for Monday", layout="wide")
st.title("Top Picks for Monday (Long Only)")

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

latest = w["date"].max()
today = w[w["date"] == latest].copy()

if today.empty:
    st.error("No rows found for latest date in weights file.")
    st.stop()

# Long-only: drop or zero shorts. Here we DROP negatives.
today = today[today["weight_tradable_v1"] > 0].copy()
if today.empty:
    st.error("No positive (long) weights for latest date.")
    st.stop()

today["abs_weight"] = today["weight_tradable_v1"].abs()

# --- Load metadata (optional) ---
try:
    ing = pd.read_csv(INGEST)
    ing.columns = [c.lower() for c in ing.columns]
    if {"ticker", "company_name", "market_sector"}.issubset(ing.columns):
        ing["ticker"] = ing["ticker"].astype(str).str.upper()
        meta = (
            ing.groupby("ticker")[["company_name", "market_sector"]]
            .first()
            .reset_index()
        )
    else:
        meta = pd.DataFrame(columns=["ticker", "company_name", "market_sector"])
except Exception:
    meta = pd.DataFrame(columns=["ticker", "company_name", "market_sector"])

top_n = st.sidebar.slider("Number of picks", 5, 50, 20)

picks = (
    today.sort_values("abs_weight", ascending=False)
    .head(top_n)
    .merge(meta, on="ticker", how="left")
)

picks["company_name"] = picks["company_name"].fillna("N/A")
picks["market_sector"] = picks["market_sector"].fillna("N/A")
picks["weight_%"] = picks["weight_tradable_v1"].map(
    lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A"
)

st.subheader(f"Run date: {latest.date()}")
st.dataframe(
    picks[["ticker", "company_name", "market_sector", "weight_%"]]
    .set_index("ticker")
)