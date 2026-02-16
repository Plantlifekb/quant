import streamlit as st
import pandas as pd
from pathlib import Path

# ============================================================
# Quant v2.0 — Signals Watchlist
# ============================================================

st.set_page_config(page_title="Signals Watchlist", layout="wide")

BASE_DIR = Path(r"C:\\Quant")
SIGNALS_FILE = BASE_DIR / "data" / "analytics" / "reporting" / "quant_signals_v1_2.csv"

# ------------------------------------------------------------
# Header
# ------------------------------------------------------------
st.title("Signals Watchlist")

st.markdown(
    """
Monitor long/short candidates, regime flags, and signal strength.

Source:
`C:\\Quant\\data\\analytics\\reporting\\quant_signals_v1_2.csv`
"""
)

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not SIGNALS_FILE.exists():
    st.warning("Signals file not found. Please run the signals engine.")
else:
    try:
        df = pd.read_csv(SIGNALS_FILE)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        st.markdown("### 🔍 Filters")

        col1, col2, col3 = st.columns(3)
        with col1:
            ticker_filter = st.text_input("Ticker contains", value="").strip()
        with col2:
            min_score = st.number_input("Min signal score", value=-5.0, step=0.5)
        with col3:
            max_score = st.number_input("Max signal score", value=5.0, step=0.5)

        filtered = df.copy()
        if ticker_filter and "ticker" in filtered.columns:
            filtered = filtered[filtered["ticker"].astype(str).str.contains(ticker_filter, case=False)]
        if "signal" in filtered.columns:
            filtered = filtered[(filtered["signal"] >= min_score) & (filtered["signal"] <= max_score)]

        st.markdown("### 📋 Watchlist")
        st.dataframe(filtered.tail(200), height=400)

    except Exception as e:
        st.error(f"Error loading signals file: {e}")