import streamlit as st
import pandas as pd
from pathlib import Path

# ============================================================
# Quant v2.0 — Dashboard Overview
# ============================================================

st.set_page_config(page_title="Dashboard Overview", layout="wide")

BASE_DIR = Path(r"C:\\Quant")
SIGNALS_FILE = BASE_DIR / "data" / "analytics" / "reporting" / "quant_signals_v1_2.csv"

# ------------------------------------------------------------
# Header
# ------------------------------------------------------------
st.title("Dashboard Overview")

st.markdown(
    """
High-level view of signal distribution, recent history, and diagnostics.

Source:
`C:\\Quant\\data\\analytics\\reporting\\quant_signals_v1_2.csv`
"""
)

# ------------------------------------------------------------
# Tabs: Signal Distribution / Validation & Diagnostics
# ------------------------------------------------------------
tab1, tab2 = st.tabs(["Signal Distribution", "Validation & Diagnostics"])

with tab1:
    st.markdown("### 📊 Signal Distribution Snapshot")

    if not SIGNALS_FILE.exists():
        st.warning("Signals file not found. Please run the signals engine.")
    else:
        try:
            df = pd.read_csv(SIGNALS_FILE)
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
            st.dataframe(df.tail(50), height=350)

            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download full signals file as CSV",
                data=csv,
                file_name="quant_signals_v1_2.csv",
                mime="text/csv",
            )
        except Exception as e:
            st.error(f"Error loading signals file: {e}")

with tab2:
    st.markdown("### 🧪 Validation & Diagnostics")
    st.info("Validation curves and diagnostics modules will be integrated here in a later release.")