import streamlit as st
import pandas as pd
import altair as alt
import math

# ------------------------------------------------------------
# Page config
# ------------------------------------------------------------
st.set_page_config(
    page_title="Quant Dashboard v1.1",
    page_icon="📊",
    layout="wide",
)

# Left-align all table cells
st.markdown("""
    <style>
        .dataframe td, .dataframe th {
            text-align: left !important;
        }
    </style>
""", unsafe_allow_html=True)

# Hide Streamlit's default sidebar navigation
st.markdown("""
    <style>
        section[data-testid="stSidebar"] div[data-testid="stSidebarNav"] {
            display: none;
        }
    </style>
""", unsafe_allow_html=True)

st.title("Quant Dashboard v1.1")

# ------------------------------------------------------------
# File paths
# ------------------------------------------------------------
PICKS_PATH = r"C:\Quant\data\analytics\_picks_v1.csv"
PERF_PATH  = r"C:\Quant\data\analytics\_perf_v1.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
@st.cache_data
def load_data():
    picks = pd.read_csv(PICKS_PATH)
    perf  = pd.read_csv(PERF_PATH)

    # Parse dates
    for df in [picks, perf]:
        if "week_start" in df.columns:
            df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")

    # Clean week_start: remove timezone if present, then convert to pure date
    for df in [picks, perf]:
        try:
            df["week_start"] = df["week_start"].dt.tz_localize(None)
        except (TypeError, AttributeError):
            pass
        df["week_start"] = df["week_start"].dt.normalize().dt.date

    # Convert weight to percent
    if "weight" in picks.columns:
        picks["weight_pct"] = (picks["weight"] * 100).round(1)

    # Ensure numeric types for performance
    for col in ["weekly_return", "cumulative_return", "drawdown"]:
        if col in perf.columns:
            perf[col] = pd.to_numeric(perf[col], errors="coerce")

    # Percent columns for charts
    perf["weekly_return_pct"] = perf["weekly_return"] * 100
    perf["drawdown_pct"] = perf["drawdown"] * 100
    perf["cumulative_return_pct"] = perf["cumulative_return"] * 100

    return picks, perf

picks, perf = load_data()

# ------------------------------------------------------------
# Sidebar
# ------------------------------------------------------------
selected_tab = st.sidebar.radio(
    "",
    ["Diagnostics", "Weekly Picks", "Performance", "Regimes", "Exposure"],
    index=0
)

# ------------------------------------------------------------
# Diagnostics
# ------------------------------------------------------------
if selected_tab == "Diagnostics":
    st.header("Diagnostics")
    st.write("Data integrity checks for picks and performance files.")

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Picks file not empty", "✓" if len(picks) > 0 else "✗")
    col2.metric("Performance file not empty", "✓" if len(perf) > 0 else "✗")
    col3.metric("regime_label present", "✓" if "regime_label" in picks.columns else "✗")
    col4.metric("weekly_return non-zero", "✓" if perf["weekly_return"].abs().sum() > 0 else "✗")
    col5.metric("multiple weeks present", "✓" if perf["week_start"].nunique() > 1 else "✗")

    st.subheader("Missing Data — Picks")
    st.dataframe(picks.isna().sum())

    st.subheader("Missing Data — Performance")
    st.dataframe(perf.isna().sum())

    st.subheader("Schema — Picks")
    st.code(list(picks.columns))

    st.subheader("Schema — Performance")
    st.code(list(perf.columns))

# ------------------------------------------------------------
# Weekly Picks (A + E)
# ------------------------------------------------------------
elif selected_tab == "Weekly Picks":
    st.header("Weekly Picks")
    st.write("Forward-looking weekly picks split by strategy.")

    latest_week = max(picks["week_start"])
    st.write(f"Showing picks for week starting: `{latest_week}`")

    latest = picks[picks["week_start"] == latest_week]

    # Long Only (strategy == long_only)
    st.subheader("Long Only Strategy")
    lo = latest[latest["strategy"] == "long_only"]
    lo_cols = ["week_start", "date", "strategy", "side", "rank", "ticker", "weight_pct"]
    lo_cols = [c for c in lo_cols if c in lo.columns]
    st.dataframe(lo.sort_values(["side", "rank"])[lo_cols])

    # Long/Short (strategy == long_short, both sides)
    st.subheader("Long/Short Strategy")
    ls = latest[latest["strategy"] == "long_short"]
    ls_cols = ["week_start", "date", "strategy", "side", "rank", "ticker", "weight_pct"]
    ls_cols = [c for c in ls_cols if c in ls.columns]
    st.dataframe(ls.sort_values(["side", "rank"])[ls_cols])

# ------------------------------------------------------------
# Performance
# ------------------------------------------------------------
elif selected_tab == "Performance":
    st.header("Performance")
    st.write("Historical performance, cumulative charts, and annual cumulative returns.")

    perf_sorted = perf.sort_values("week_start")
    perf_sorted["year"] = pd.to_datetime(perf_sorted["week_start"]).dt.year

    # Weekly returns chart
    st.subheader("Weekly Returns (%)")

    min_y = perf_sorted["weekly_return_pct"].min()
    max_y = perf_sorted["weekly_return_pct"].max()
    padding = 2

    chart_weekly = alt.Chart(perf_sorted).mark_line().encode(
        x="week_start:T",
        y=alt.Y(
            "weekly_return_pct:Q",
            scale=alt.Scale(domain=[min_y - padding, max_y + padding])
        ),
        color="strategy:N"
    )
    st.altair_chart(chart_weekly, use_container_width=True)

    # Annual cumulative charts
    st.header("Annual Cumulative Returns (Charts)")

    for yr in sorted(perf_sorted["year"].unique()):
        df_year = perf_sorted[perf_sorted["year"] == yr].copy()
        df_year = df_year.sort_values("week_start")

        st.subheader(f"Cumulative Returns — {yr}")

        chart_year = (
            alt.Chart(df_year)
               .mark_line()
               .encode(
                   x="week_start:T",
                   y="cumulative_return_pct:Q",
                   color="strategy:N"
               )
        )

        st.altair_chart(chart_year, use_container_width=True)

    # Unified Annual Cumulative Returns Table
    st.header("Annual Cumulative Returns — Unified Table")

    def cumulative_for_year(df):
        r = df["weekly_return"].dropna()
        if len(r) == 0:
            return math.nan
        return (1.0 + r).prod() - 1.0

    annual_cum = (
        perf_sorted.groupby(["strategy", "year"])
                   .apply(cumulative_for_year)
                   .reset_index(name="CumulativeReturn")
    )

    # Only long_only and long_short
    annual_cum = annual_cum[annual_cum["strategy"].isin(["long_only", "long_short"])]

    # Percent
    annual_cum["CumulativeReturn %"] = (annual_cum["CumulativeReturn"] * 100).round(1)

    # Pivot to unified table: one row per year, columns for each strategy
    annual_unified = annual_cum.pivot(index="year", columns="strategy", values="CumulativeReturn %")

    # Ensure both columns exist
    for col in ["long_only", "long_short"]:
        if col not in annual_unified.columns:
            annual_unified[col] = math.nan

    annual_unified = annual_unified.reset_index().sort_values("year")

    st.dataframe(annual_unified)

# ------------------------------------------------------------
# Regimes
# ------------------------------------------------------------
elif selected_tab == "Regimes":
    st.header("Regimes")
    st.write("Historical and current regime distributions.")

    if "regime_label" in picks.columns:

        regime_counts = (
            picks.groupby(["week_start", "regime_label"])
                 .size()
                 .unstack(fill_value=0)
        )

        chart_regime = (
            alt.Chart(regime_counts.reset_index())
               .transform_fold(
                   regime_counts.columns.tolist(),
                   as_=["regime", "count"]
               )
               .mark_area()
               .encode(
                   x="week_start:T",
                   y="count:Q",
                   color="regime:N"
               )
        )

        st.subheader("Historical Regime Breakdown")
        st.altair_chart(chart_regime, use_container_width=True)

        latest_week = max(picks["week_start"])
        current_week = picks[picks["week_start"] == latest_week]

        current_counts = (
            current_week.groupby("regime_label")
                        .size()
                        .reset_index(name="count")
        )

        chart_current = (
            alt.Chart(current_counts)
               .mark_bar()
               .encode(
                   x="regime_label:N",
                   y="count:Q",
                   color="regime_label:N"
               )
        )

        st.subheader(f"Current Regimes — Week of {latest_week}")
        st.altair_chart(chart_current, use_container_width=True)

    else:
        st.warning("No regime_label column found in picks file.")

# ------------------------------------------------------------
# Exposure
# ------------------------------------------------------------
elif selected_tab == "Exposure":
    st.header("Exposure")
    st.write("Long and short exposure over time.")

    exposure = (
        picks.groupby(["week_start", "side"])["weight_pct"]
             .sum()
             .unstack(fill_value=0)
    )

    chart_exp = (
        alt.Chart(exposure.reset_index())
           .transform_fold(
               list(exposure.columns),
               as_=["side", "exposure"]
           )
           .mark_line()
           .encode(
               x="week_start:T",
               y=alt.Y("exposure:Q", scale=alt.Scale(domain=[-100, 200])),
               color="side:N"
           )
    )

    st.subheader("Long vs Short Exposure (%)")
    st.altair_chart(chart_exp, use_container_width=True)