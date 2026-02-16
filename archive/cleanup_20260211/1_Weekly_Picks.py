import pandas as pd
import streamlit as st
from pathlib import Path


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
BASE_DIR = Path(r"C:\Quant")
DATA_ANALYTICS = BASE_DIR / "data" / "analytics"
WEEKLY_PICKS_FILE = DATA_ANALYTICS / "quant_weekly_picks_quant_v1.parquet"


# ----------------------------------------------------------------------
# Data loading
# ----------------------------------------------------------------------
@st.cache_data
def load_weekly_picks() -> pd.DataFrame:
    if not WEEKLY_PICKS_FILE.exists():
        raise FileNotFoundError(
            f"Weekly picks file not found: {WEEKLY_PICKS_FILE}\n"
            "Ensure master_pipeline_quant_v1 has run and "
            "weekly_picks_quant_v1.py has produced output."
        )

    df = pd.read_parquet(WEEKLY_PICKS_FILE)

    if "as_of_date" not in df.columns:
        raise KeyError("Expected 'as_of_date' column in weekly picks file")

    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


# ----------------------------------------------------------------------
# Layout
# ----------------------------------------------------------------------
st.title("Weekly Picks – Quant Optimiser")

try:
    df = load_weekly_picks()
except Exception as e:
    st.error(f"Unable to load weekly picks:\n\n{e}")
    st.stop()

latest_date = df["as_of_date"].max()
df_latest = df[df["as_of_date"] == latest_date].copy()
df_latest = df_latest.sort_values("rank")

st.subheader(f"Top {len(df_latest)} picks as of {latest_date.date()}")

# Core table: rank, ticker, name, sector, weight
display_cols = ["rank", "ticker", "company_name", "sector", "weight"]
existing_cols = [c for c in display_cols if c in df_latest.columns]

st.dataframe(
    df_latest[existing_cols],
    hide_index=True,
    use_container_width=True,
)

# Optional: simple summary
if "weight" in df_latest.columns:
    total_weight = df_latest["weight"].sum()
    st.caption(f"Total weight of displayed picks: {total_weight:.2%}")