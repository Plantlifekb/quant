import streamlit as st
import pandas as pd
from pathlib import Path

# ============================================================
# Quant v2.0 — Ticker Intelligence
# ============================================================

st.set_page_config(page_title="Ticker Intelligence", layout="wide")

BASE_DIR = Path(r"C:\\Quant")
SIGNALS_FILE = BASE_DIR / "data" / "analytics" / "reporting" / "quant_signals_v1_2.csv"

# ------------------------------------------------------------
# Header
# ------------------------------------------------------------
st.title("Ticker Intelligence")

st.markdown(
    """
Ticker-level snapshots, history, and basic peer comparison.

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

        if "ticker" not in df.columns:
            st.error("No 'ticker' column found in signals file.")
        else:
            tickers = sorted(df["ticker"].dropna().unique().tolist())
            selected = st.selectbox("Select ticker", tickers)

            tdf = df[df["ticker"] == selected].sort_values("date")

            st.markdown(f"### 📈 History for {selected}")
            st.dataframe(tdf.tail(200), height=400)

            if "signal" in tdf.columns and "date" in tdf.columns:
                st.line_chart(tdf.set_index("date")["signal"])

    except Exception as e:
        st.error(f"Error loading signals file: {e}")