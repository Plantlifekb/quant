import streamlit as st
import pandas as pd
from pathlib import Path

# ============================================================
# Quant v2.0 — Strategy Performance
# ============================================================

st.set_page_config(page_title="Strategy Performance", layout="wide")

BASE_DIR = Path(r"C:\\Quant")
PERF_FILE = BASE_DIR / "data" / "analytics" / "reporting" / "strategy_performance.csv"

# ------------------------------------------------------------
# Header
# ------------------------------------------------------------
st.title("Strategy Performance")

st.markdown(
    """
Performance overview for the governed strategy backtest:
5-year cumulative return and average periodic returns.

Expected source:
`C:\\Quant\\data\\analytics\\reporting\\strategy_performance.csv`
"""
)

# ------------------------------------------------------------
# Load Data and Metrics
# ------------------------------------------------------------
if not PERF_FILE.exists():
    st.info("Performance file not found yet. Once the backtest is wired, this page will display full metrics.")
else:
    try:
        df = pd.read_csv(PERF_FILE)

        if "date" not in df.columns or "return" not in df.columns:
            st.error("Performance file must contain 'date' and 'return' columns.")
        else:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date")

            # 5‑Year Cumulative Return
            st.markdown("### 📈 5‑Year Cumulative Return")
            df["cum_return"] = (1 + df["return"]).cumprod() - 1
            st.line_chart(df.set_index("date")["cum_return"])

            # Average Returns
            st.markdown("### 📊 Average Returns")

            avg_daily = df["return"].mean() * 100
            weekly = df.resample("W", on="date")["return"].sum()
            monthly = df.resample("M", on="date")["return"].sum()
            annual = df.resample("Y", on="date")["return"].sum()

            avg_weekly = weekly.mean() * 100
            avg_monthly = monthly.mean() * 100
            avg_annual = annual.mean() * 100

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Average Daily Return", f"{avg_daily:.1f}%")
            c2.metric("Average Weekly Return", f"{avg_weekly:.1f}%")
            c3.metric("Average Monthly Return", f"{avg_monthly:.1f}%")
            c4.metric("Average Annual Return", f"{avg_annual:.1f}%")

    except Exception as e:
        st.error(f"Error loading performance file: {e}")