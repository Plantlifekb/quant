import pandas as pd
import streamlit as st
from pathlib import Path
import altair as alt

# Paths
REPORT_DIR = Path(r"C:\Quant\data\analytics\reporting")
DASHBOARD_INPUTS_FILE = REPORT_DIR / "quant_dashboard_inputs_v1.csv"
STACKED_REPORT_FILE = REPORT_DIR / "quant_report_v1.csv"

# Load data
@st.cache_data
def load_dashboard_inputs():
    if not DASHBOARD_INPUTS_FILE.exists():
        return pd.DataFrame()
    df = pd.read_csv(DASHBOARD_INPUTS_FILE)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

@st.cache_data
def load_stacked_report():
    if not STACKED_REPORT_FILE.exists():
        return pd.DataFrame()
    df = pd.read_csv(STACKED_REPORT_FILE)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

df_dash = load_dashboard_inputs()
df_report = load_stacked_report()

st.set_page_config(page_title="Quant v1.1 Dashboard", layout="wide")
st.title("Quant v1.1 — Portfolio Analytics Dashboard")

if df_dash.empty:
    st.error("Dashboard inputs file is empty or missing.")
    st.stop()

# Sidebar filters
min_date = df_dash["date"].min()
max_date = df_dash["date"].max()

with st.sidebar:
    st.header("Filters")
    date_range = st.date_input(
        "Date range",
        value=(min_date.date(), max_date.date()),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )

    if isinstance(date_range, tuple):
        start_date, end_date = date_range
    else:
        start_date, end_date = date_range, date_range

mask = (df_dash["date"].dt.date >= start_date) & (df_dash["date"].dt.date <= end_date)
df_view = df_dash.loc[mask].sort_values("date")

# Top bar metrics
latest_row = df_view.sort_values("date").iloc[-1]

col1, col2, col3, col4 = st.columns(4)
col5, col6, col7, col8 = st.columns(4)

col1.metric("Date", latest_row["date"].strftime("%Y-%m-%d"))
if "pnl" in latest_row.index:
    col2.metric("PNL", f"{latest_row['pnl']:.2f}")
col3.metric("Daily Volatility", f"{latest_row['daily_volatility']:.4f}")
col4.metric("Factor Risk", f"{latest_row['factor_risk']:.4f}")
col5.metric("Liquidity Risk", f"{latest_row['liquidity_risk']:.4f}")
col6.metric("Turnover Risk", f"{latest_row['turnover_risk']:.2f}")
col7.metric("Regime Risk", f"{latest_row['regime_risk']:.4f}")
if "rolling_volatility_mean" in latest_row.index:
    col8.metric("Rolling Volatility (mean)", f"{latest_row['rolling_volatility_mean']:.4f}")

st.markdown("---")

# Time-series chart
st.subheader("Time-Series: PNL, Volatility, Risk")
ts_cols = ["pnl", "daily_volatility", "factor_risk", "liquidity_risk", "turnover_risk", "regime_risk"]
ts_cols = [c for c in ts_cols if c in df_view.columns]

if ts_cols:
    df_ts = df_view[["date"] + ts_cols].melt("date", var_name="metric", value_name="value")
    chart_ts = (
        alt.Chart(df_ts)
        .mark_line()
        .encode(x="date:T", y="value:Q", color="metric:N", tooltip=["date:T", "metric:N", "value:Q"])
        .properties(height=300)
        .interactive()
    )
    st.altair_chart(chart_ts, use_container_width=True)
else:
    st.info("No time-series metrics available to plot.")

st.markdown("---")

# Rolling risk chart
st.subheader("Rolling Risk Metrics")
roll_cols = [
    "rolling_volatility_mean",
    "rolling_factor_risk_mean",
    "rolling_beta_mean",
    "rolling_turnover_pressure_mean",
    "rolling_liquidity_pressure_mean",
]
roll_cols = [c for c in roll_cols if c in df_view.columns]

if roll_cols:
    df_roll = df_view[["date"] + roll_cols].melt("date", var_name="metric", value_name="value")
    chart_roll = (
        alt.Chart(df_roll)
        .mark_line()
        .encode(x="date:T", y="value:Q", color="metric:N", tooltip=["date:T", "metric:N", "value:Q"])
        .properties(height=300)
        .interactive()
    )
    st.altair_chart(chart_roll, use_container_width=True)
else:
    st.info("No rolling risk metrics available to plot.")

st.markdown("---")

# Regime intelligence
st.subheader("Regime Intelligence")
if not df_report.empty and "REPORT_SECTION" in df_report.columns:
    reg_mask = df_report["REPORT_SECTION"] == "REPORT_REGIME_SUMMARY"
    df_reg = df_report[reg_mask].copy()
    if not df_reg.empty:
        st.write("Regime summary (raw view):")
        st.dataframe(df_reg)
    else:
        st.info("No regime summary section found in stacked report.")
else:
    st.info("Stacked report not available or missing REPORT_SECTION column.")

# Raw data preview
with st.expander("Raw dashboard data (current date range)"):
    st.dataframe(df_view.reset_index(drop=True))