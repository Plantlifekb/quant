import streamlit as st
import pandas as pd
import numpy as np

# ---------------------------------------------------------
# PATHS
# ---------------------------------------------------------

DASHBOARD_PATH = r"C:\Quant\data\dashboard\dashboard_10stock_weekly.csv"
BACKTEST_PATH = r"C:\Quant\data\backtest\backtest_10stock_weekly_regime.csv"
REGIME_PATH = r"C:\Quant\data\analytics\quant_regime_states_v1.csv"
ATTRIB_REGIME_PATH = r"C:\Quant\data\analytics\quant_attribution_regime_v1.csv"

st.set_page_config(page_title="10-Stock Weekly Dashboard (Regime-Aware)", layout="wide")

# ---------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------

@st.cache_data
def load_data():
    dash = pd.read_csv(DASHBOARD_PATH, parse_dates=["date"])
    bt = pd.read_csv(BACKTEST_PATH, parse_dates=["date"])
    regime = pd.read_csv(REGIME_PATH, parse_dates=["date"])
    attrib = pd.read_csv(ATTRIB_REGIME_PATH, parse_dates=["date"])
    return dash, bt, regime, attrib

dash, bt, regime, attrib = load_data()

# ---------------------------------------------------------
# FIX DATE TYPES
# ---------------------------------------------------------

bt["date"] = pd.to_datetime(bt["date"]).dt.tz_localize(None)
regime["date"] = pd.to_datetime(regime["date"]).dt.tz_localize(None)
attrib["date"] = pd.to_datetime(attrib["date"]).dt.tz_localize(None)

# ---------------------------------------------------------
# NORMALISE REGIME COLUMN
# ---------------------------------------------------------

if "regime_label" in regime.columns:
    regime = regime.rename(columns={"regime_label": "regime"})
else:
    st.error("No regime_label column found in regime file.")
    st.stop()

bt = bt.sort_values("date").reset_index(drop=True)
regime = regime.sort_values("date").reset_index(drop=True)

# ---------------------------------------------------------
# MERGE REGIME WITH BACKTEST
# ---------------------------------------------------------

bt_regime = pd.merge_asof(
    bt.sort_values("date"),
    regime[["date", "regime"]].sort_values("date"),
    on="date",
    direction="backward"
)

bt_regime["regime"] = bt_regime["regime"].fillna("unknown")

# ---------------------------------------------------------
# PERFORMANCE METRICS (REGIME-AWARE)
# ---------------------------------------------------------

weekly_ret = bt_regime["portfolio_weekly_return_regime"]
bt_regime["cum"] = (1 + weekly_ret).cumprod()

avg_weekly = weekly_ret.mean()

bt_regime["month"] = bt_regime["date"].dt.to_period("M")
monthly_returns = bt_regime.groupby("month")["portfolio_weekly_return_regime"].apply(lambda x: (1 + x).prod() - 1)
avg_monthly = monthly_returns.mean()

bt_regime["year"] = bt_regime["date"].dt.year
annual_returns = bt_regime.groupby("year")["portfolio_weekly_return_regime"].apply(lambda x: (1 + x).prod() - 1)
avg_annual = annual_returns.mean()

def trailing_return(days):
    cutoff = bt_regime["date"].max() - pd.Timedelta(days=days)
    subset = bt_regime[bt_regime["date"] >= cutoff]
    if len(subset) < 2:
        return np.nan
    return subset["cum"].iloc[-1] / subset["cum"].iloc[0] - 1

trailing_1m = trailing_return(30)
trailing_3m = trailing_return(90)
trailing_12m = trailing_return(365)

current_year = bt_regime["date"].max().year
ytd_subset = bt_regime[bt_regime["year"] == current_year]
ytd_return = ytd_subset["cum"].iloc[-1] / ytd_subset["cum"].iloc[0] - 1 if len(ytd_subset) > 1 else np.nan

years = (bt_regime["date"].max() - bt_regime["date"].min()).days / 365
cagr = (1 + bt_regime["cum"].iloc[-1]) ** (1 / years) - 1 if years > 0 else np.nan

roll_max = bt_regime["cum"].cummax()
drawdown = (bt_regime["cum"] - roll_max) / roll_max
max_dd = drawdown.min()

weekly_std = weekly_ret.std()
weekly_mean = weekly_ret.mean()
sharpe = (weekly_mean / weekly_std * np.sqrt(52)) if weekly_std > 0 else np.nan

# ---------------------------------------------------------
# REGIME PERFORMANCE
# ---------------------------------------------------------

regime_perf = []
for reg, grp in bt_regime.groupby("regime"):
    if len(grp) < 2:
        continue
    reg_cum = (1 + grp["portfolio_weekly_return_regime"]).cumprod()
    reg_return = reg_cum.iloc[-1] - 1
    reg_std = grp["portfolio_weekly_return_regime"].std()
    reg_sharpe = (grp["portfolio_weekly_return_regime"].mean() / reg_std * np.sqrt(52)) if reg_std > 0 else np.nan
    reg_dd = ((reg_cum - reg_cum.cummax()) / reg_cum.cummax()).min()
    regime_perf.append({
        "regime": reg,
        "return": reg_return,
        "sharpe": reg_sharpe,
        "max_drawdown": reg_dd
    })

regime_perf_df = pd.DataFrame(regime_perf).sort_values("return", ascending=False)
current_regime = regime.sort_values("date").iloc[-1]["regime"]

# ---------------------------------------------------------
# UI — PERFORMANCE SUMMARY
# ---------------------------------------------------------

st.title("📈 10-Stock Weekly Strategy Dashboard (Regime-Aware)")

st.header("📊 Performance Summary (5-Year Averages)")

col1, col2, col3 = st.columns(3)
col1.metric("Avg Weekly Return", f"{avg_weekly:.2%}")
col2.metric("Avg Monthly Return", f"{avg_monthly:.2%}")
col3.metric("Avg Annual Return", f"{avg_annual:.2%}")

col4, col5, col6 = st.columns(3)
col4.metric("Trailing 1-Month", f"{trailing_1m:.2%}")
col5.metric("Trailing 3-Month", f"{trailing_3m:.2%}")
col6.metric("Trailing 12-Month", f"{trailing_12m:.2%}")

col7, col8, col9 = st.columns(3)
col7.metric("YTD Return", f"{ytd_return:.2%}" if not np.isnan(ytd_return) else "N/A")
col8.metric("CAGR", f"{cagr:.2%}" if not np.isnan(cagr) else "N/A")
col9.metric("Max Drawdown", f"{max_dd:.2%}")

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
    display_df["max_drawdown"] = display_df["max_drawdown"].map(lambda x: f"{x:.2%}")
    st.dataframe(display_df.reset_index(drop=True))

# ---------------------------------------------------------
# EXPOSURE PANEL
# ---------------------------------------------------------

st.subheader("Exposure by Regime")
if "exposure" in bt_regime.columns:
    exp_table = bt_regime.groupby("regime")["exposure"].mean().reset_index()
    exp_table["exposure"] = exp_table["exposure"].map(lambda x: f"{x:.0%}")
    st.dataframe(exp_table)

# ---------------------------------------------------------
# MONDAY PICKS
# ---------------------------------------------------------

latest_date = dash["date"].max()
latest_picks = dash[dash["date"] == latest_date]

st.header(f"📅 Monday Picks — {latest_date.date()}")
st.dataframe(
    latest_picks[["ticker", "score", "weight"]]
    .sort_values("score", ascending=False)
    .reset_index(drop=True)
)

# ---------------------------------------------------------
# EQUITY CURVES
# ---------------------------------------------------------

st.header("📈 Regime-Aware Equity Curve")

cum_df = bt_regime[["date", "cum", "regime"]].set_index("date")
st.line_chart(cum_df["cum"])

st.subheader("Regime Timeline")
timeline = bt_regime[["date", "regime"]].copy()
timeline["value"] = 1.0
pivot = timeline.pivot(index="date", columns="regime", values="value").fillna(0)
st.area_chart(pivot)

# ---------------------------------------------------------
# WEEKLY RETURNS + DRAWDOWN
# ---------------------------------------------------------

st.subheader("Weekly Returns (Regime-Aware)")
st.bar_chart(bt_regime.set_index("date")["portfolio_weekly_return_regime"])

st.subheader("Drawdown")
drawdown_df = pd.DataFrame({"date": bt_regime["date"], "drawdown": drawdown}).set_index("date")
st.area_chart(drawdown_df)

# ---------------------------------------------------------
# REGIME-AWARE ATTRIBUTION
# ---------------------------------------------------------

st.header("🧩 Regime-Aware Attribution")

if {"regime", "factor", "contribution"}.issubset(attrib.columns):
    summary = (
        attrib.groupby(["regime", "factor"])["contribution"]
        .sum()
        .reset_index()
        .sort_values("contribution", ascending=False)
    )
    st.dataframe(summary)
else:
    st.write("Attribution file loaded, but expected columns not found.")

# ---------------------------------------------------------
# FULL HISTORY
# ---------------------------------------------------------

st.header("📜 Full Weekly History")
st.dataframe(
    dash.sort_values(["date", "score"], ascending=[False, False]).reset_index(drop=True),
    height=600
)