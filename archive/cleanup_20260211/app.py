import streamlit as st
import pandas as pd
import numpy as np

# ---------------------------------------------------------
# DATA SOURCES — CANONICAL PORTFOLIO PERFORMANCE + REGIMES
# ---------------------------------------------------------

PERF_PATH   = r"C:\Quant\data\analytics\quant_portfolio_performance.csv"
REGIME_PATH = r"C:\Quant\data\analytics\quant_regime_states_v1.csv"

st.set_page_config(page_title="10-Stock Weekly Strategy Dashboard", layout="wide")

# ---------------------------------------------------------
# LOAD + BUILD WEEKLY BACKTEST FROM CANONICAL PERFORMANCE
# ---------------------------------------------------------

@st.cache_data
def load_data():
    perf = pd.read_csv(PERF_PATH)

    # Parse dates with mixed format fallback
    perf["date"] = pd.to_datetime(
        perf["date"],
        format="mixed",
        dayfirst=True,
        errors="coerce"
    ).dt.tz_localize(None)

    perf = perf.dropna(subset=["date"])

    # Filter to long-only portfolio
    if "portfolio_type" in perf.columns:
        perf = perf[perf["portfolio_type"] == "longonly"].copy()

    # Identify daily return column
    return_col = None
    for candidate in ["daily_return", "portfolio_return", "return"]:
        if candidate in perf.columns:
            return_col = candidate
            break

    if return_col is None:
        st.error("No daily return column found in quant_portfolio_performance.csv.")
        st.stop()

    perf = perf[["date", return_col]].rename(columns={return_col: "daily_return"})
    perf = perf.sort_values("date").reset_index(drop=True)

    # Rebuild cumulative from daily returns
    perf["cum_from_daily"] = (1 + perf["daily_return"]).cumprod()

    # Convert to weekly (Friday close)
    perf["week"] = perf["date"].dt.to_period("W")
    weekly = (
        perf.groupby("week")["daily_return"]
        .apply(lambda x: (1 + x).prod() - 1)
        .reset_index()
    )
    weekly["date"] = weekly["week"].dt.end_time
    weekly = weekly[["date", "daily_return"]].rename(
        columns={"daily_return": "portfolio_weekly_return"}
    )
    weekly = weekly.sort_values("date").reset_index(drop=True)
    weekly["cumulative_return"] = (1 + weekly["portfolio_weekly_return"]).cumprod()

    # Load regimes
    regime = pd.read_csv(REGIME_PATH)
    regime["date"] = pd.to_datetime(
        regime["date"],
        format="mixed",
        dayfirst=True,
        errors="coerce"
    ).dt.tz_localize(None)
    regime = regime.dropna(subset=["date"])

    if "regime_label" in regime.columns:
        regime = regime.rename(columns={"regime_label": "regime"})
    elif "regime" not in regime.columns:
        st.error("No regime column found in regime file.")
        st.stop()

    regime = regime[["date", "regime"]].sort_values("date").reset_index(drop=True)

    return weekly, regime


bt, regime = load_data()

# ---------------------------------------------------------
# ALIGN DATES + MERGE REGIMES
# ---------------------------------------------------------

bt["date"] = pd.to_datetime(bt["date"])
regime["date"] = pd.to_datetime(regime["date"])

bt = bt.sort_values("date").reset_index(drop=True)
regime = regime.sort_values("date").reset_index(drop=True)

bt_regime = pd.merge_asof(
    bt.sort_values("date"),
    regime.sort_values("date"),
    on="date",
    direction="backward"
)
bt_regime["regime"] = bt_regime["regime"].fillna("unknown")

# ---------------------------------------------------------
# PERFORMANCE METRICS
# ---------------------------------------------------------

bt["month"] = bt["date"].dt.to_period("M")
bt["year"] = bt["date"].dt.year
bt["cum"] = (1 + bt["portfolio_weekly_return"]).cumprod()

avg_weekly = bt["portfolio_weekly_return"].mean()

monthly_returns = bt.groupby("month")["portfolio_weekly_return"].apply(
    lambda x: (1 + x).prod() - 1
)
avg_monthly = monthly_returns.mean()

annual_returns = bt.groupby("year")["portfolio_weekly_return"].apply(
    lambda x: (1 + x).prod() - 1
)
avg_annual = annual_returns.mean()

def trailing_return(days: int):
    cutoff = bt["date"].max() - pd.Timedelta(days=days)
    subset = bt[bt["date"] >= cutoff]
    if len(subset) < 2:
        return np.nan
    return subset["cum"].iloc[-1] / subset["cum"].iloc[0] - 1

trailing_1m = trailing_return(30)
trailing_3m = trailing_return(90)
trailing_12m = trailing_return(365)

current_year = bt["date"].max().year
ytd_subset = bt[bt["year"] == current_year]
ytd_return = (
    ytd_subset["cum"].iloc[-1] / ytd_subset["cum"].iloc[0] - 1
    if len(ytd_subset) > 1 else np.nan
)

years = (bt["date"].max() - bt["date"].min()).days / 365
cagr = (
    (1 + bt["cumulative_return"].iloc[-1]) ** (1 / years) - 1
    if years > 0 else np.nan
)

cum = bt["cum"]
roll_max = cum.cummax()
drawdown = (cum - roll_max) / roll_max
max_dd = drawdown.min()

weekly_std = bt["portfolio_weekly_return"].std()
weekly_mean = bt["portfolio_weekly_return"].mean()
sharpe = (weekly_mean / weekly_std * np.sqrt(52)) if weekly_std > 0 else np.nan

# ---------------------------------------------------------
# REGIME PERFORMANCE
# ---------------------------------------------------------

regime_perf = []
for reg, grp in bt_regime.groupby("regime"):
    if len(grp) < 2:
        continue
    reg_cum = (1 + grp["portfolio_weekly_return"]).cumprod()
    reg_return = reg_cum.iloc[-1] - 1
    reg_std = grp["portfolio_weekly_return"].std()
    reg_sharpe = (
        grp["portfolio_weekly_return"].mean() / reg_std * np.sqrt(52)
        if reg_std > 0 else np.nan
    )
    reg_dd = ((reg_cum - reg_cum.cummax()) / reg_cum.cummax()).min()
    regime_perf.append({
        "regime": reg,
        "return": reg_return,
        "sharpe": reg_sharpe,
        "max_drawdown": reg_dd
    })

regime_perf_df = pd.DataFrame(regime_perf)
current_regime = (
    regime.sort_values("date").iloc[-1]["regime"]
    if not regime.empty else "unknown"
)

# ---------------------------------------------------------
# DASHBOARD UI
# ---------------------------------------------------------

st.title("📈 10-Stock Weekly Strategy Dashboard (Regime-Aware)")

st.header("📊 Performance Summary")

col1, col2, col3 = st.columns(3)
col1.metric("Avg Weekly Return", f"{avg_weekly:.2%}")
col2.metric("Avg Monthly Return", f"{avg_monthly:.2%}")
col3.metric("Avg Annual Return", f"{avg_annual:.2%}")

col4, col5, col6 = st.columns(3)
col4.metric("Trailing 1-Month", f"{trailing_1m:.2%}" if not np.isnan(trailing_1m) else "N/A")
col5.metric("Trailing 3-Month", f"{trailing_3m:.2%}" if not np.isnan(trailing_3m) else "N/A")
col6.metric("Trailing 12-Month", f"{trailing_12m:.2%}" if not np.isnan(trailing_12m) else "N/A")

col7, col8, col9 = st.columns(3)
col7.metric("YTD Return", f"{ytd_return:.2%}" if not np.isnan(ytd_return) else "N/A")
col8.metric("CAGR", f"{cagr:.2%}" if not np.isnan(cagr) else "N/A")
col9.metric("Max Drawdown", f"{max_dd:.2%}" if not np.isnan(max_dd) else "N/A")

st.metric("Sharpe Ratio (Annualised)", f"{sharpe:.2f}" if not np.isnan(sharpe) else "N/A")

# ---------------------------------------------------------
# REGIME PANEL
# ---------------------------------------------------------

st.header("🧭 Regime Overview")

colr1, colr2 = st.columns([1, 2])
colr1.metric("Current Regime", str(current_regime))

colr2.subheader("Performance by Regime")
if not regime_perf_df.empty:
    display_df = regime_perf_df.copy()
    display_df["return"] = display_df["return"].map(lambda x: f"{x:.2%}")
    display_df["sharpe"] = display_df["sharpe"].map(lambda x: f"{x:.2f}" if pd.notnull(x) else "N/A")
    display_df["max_drawdown"] = display_df["max_drawdown"].map(lambda x: f"{x:.2%}" if pd.notnull(x) else "N/A")
    st.dataframe(display_df.reset_index(drop=True))
else:
    st.write("No regime performance data available.")

# ---------------------------------------------------------
# PERFORMANCE CHARTS
# ---------------------------------------------------------

st.header("📈 Performance")

st.subheader("Cumulative Return (with Regime)")
cum_df = bt_regime[["date", "cumulative_return", "regime"]].set_index("date")
st.line_chart(cum_df["cumulative_return"])

st.subheader("Regime Timeline")
timeline = bt_regime[["date", "regime"]].copy()
timeline["value"] = 1.0
pivot = timeline.pivot(index="date", columns="regime", values="value").fillna(0)
st.area_chart(pivot)

st.subheader("Weekly Returns")
st.bar_chart(bt.set_index("date")["portfolio_weekly_return"])

st.subheader("Drawdown")
drawdown_df = pd.DataFrame({"date": bt["date"], "drawdown": drawdown}).set_index("date")
st.area_chart(drawdown_df)

# ---------------------------------------------------------
# FULL HISTORY
# ---------------------------------------------------------

st.header("📜 Full Weekly History")
st.dataframe(
    bt.sort_values(["date"], ascending=[False]).reset_index(drop=True),
    height=600
)