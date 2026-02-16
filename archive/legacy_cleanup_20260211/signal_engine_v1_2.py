import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import plotly.express as px

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
BASE_DIR = Path(r"C:\Quant")
REPORT_DIR = BASE_DIR / "data" / "analytics" / "reporting"
SIGNALS_FILE = REPORT_DIR / "quant_signals_v1_2.csv"
REPORT_FILE = REPORT_DIR / "quant_report_v1.csv"

# ------------------------------------------------------------
# Banner
# ------------------------------------------------------------
st.markdown(
    """
    <div style='text-align:center; padding:15px; background-color:#f0f8ff; border-radius:8px;'>
        <h1 style='color:#003366;'>Quant v2.0 — Predictive Cockpit</h1>
        <h4 style='color:#0066CC;'>Weekly Rebalance · Regime-Aware · Multi-Stage Signal Engine</h4>
        <p style='color:#333333; font-size:15px; max-width:900px; margin:auto;'>
            This cockpit provides a governed, audit-ready view of the latest market signals, attribution metrics,
            and strategy diagnostics. It is designed for weekly decision-making, regime-aware positioning, and
            multi-factor signal validation, with hard integrity checks to prevent decisions on broken or stale data.
        </p>
    </div>
    """,
    unsafe_allow_html=True
)

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
@st.cache_data
def load_signals():
    if not SIGNALS_FILE.exists():
        return pd.DataFrame()
    df = pd.read_csv(SIGNALS_FILE)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

@st.cache_data
def load_report():
    if not REPORT_FILE.exists():
        return pd.DataFrame()
    df = pd.read_csv(REPORT_FILE)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

signals = load_signals()
report = load_report()

# ------------------------------------------------------------
# Integrity helpers
# ------------------------------------------------------------
def find_latest_usable_date(signals: pd.DataFrame, report: pd.DataFrame):
    """
    Find the latest date where:
      - attribution section exists
      - core attribution fields are non-null for at least some rows
      - signals exist
      - signal_norm is non-null and non-zero for at least some rows
    This automatically skips weekends / dead dates.
    """
    if signals.empty or report.empty:
        return None

    if "REPORT_SECTION" not in report.columns:
        return None

    attr = report[report["REPORT_SECTION"] == "REPORT_ATTRIBUTION_ROLLING"].copy()
    if attr.empty:
        return None

    core_cols = ["ticker", "contribution", "rolling_beta", "rolling_volatility"]
    for c in core_cols:
        if c not in attr.columns:
            return None

    attr["attr_ok"] = attr[core_cols].notnull().all(axis=1)
    attr_ok_dates = attr[attr["attr_ok"]].groupby(attr["date"].dt.date).size().index

    if "signal_norm" not in signals.columns:
        return None
    sig = signals.copy()
    sig["sig_ok"] = sig["signal_norm"].notnull() & (signals["signal_norm"] != 0)
    sig_ok_dates = sig[sig["sig_ok"]].groupby(sig["date"].dt.date).size().index

    good_dates = sorted(set(attr_ok_dates).intersection(set(sig_ok_dates)))
    if not good_dates:
        return None
    return good_dates[-1]

def compute_integrity(signals: pd.DataFrame, report: pd.DataFrame, latest_date):
    status = {
        "has_signals_file": not signals.empty,
        "has_report_file": not report.empty,
        "latest_date": latest_date,
        "attr_rows_for_latest": False,
        "attr_core_non_null": False,
        "signals_for_latest": False,
        "signals_non_null": False,
        "signals_non_zero": False,
    }
    if latest_date is None or signals.empty or report.empty:
        return status

    attr = report[report["REPORT_SECTION"] == "REPORT_ATTRIBUTION_ROLLING"]
    attr_day = attr[attr["date"].dt.date == latest_date]
    status["attr_rows_for_latest"] = not attr_day.empty

    core_cols = ["ticker", "contribution", "rolling_beta", "rolling_volatility"]
    if not attr_day.empty and all(c in attr_day.columns for c in core_cols):
        non_null_mask = attr_day[core_cols].notnull().all(axis=1)
        status["attr_core_non_null"] = non_null_mask.any()

    sig_day = signals[signals["date"].dt.date == latest_date]
    status["signals_for_latest"] = not sig_day.empty

    if not sig_day.empty and "signal_norm" in sig_day.columns:
        status["signals_non_null"] = sig_day["signal_norm"].notnull().any()
        status["signals_non_zero"] = (sig_day["signal_norm"].fillna(0) != 0).any()

    return status

latest_usable_date = find_latest_usable_date(signals, report)
integrity = compute_integrity(signals, report, latest_usable_date)

if latest_usable_date is None:
    st.error(
        "No usable date found with both valid attribution and non-zero signals.\n\n"
        "Run attribution, reporting, and signal engines for a recent trading day."
    )
    st.stop()

# ------------------------------------------------------------
# Regime + header metrics
# ------------------------------------------------------------
report_day = report[report["date"].dt.date == latest_usable_date]

if "regime_label" in report_day.columns:
    vals = report_day["regime_label"].dropna().unique()
    regime = vals[0] if len(vals) > 0 else "unknown"
else:
    regime = "unknown"

df_day_signals = signals[signals["date"].dt.date == latest_usable_date].copy()
universe_size = df_day_signals["ticker"].nunique()

colA, colB, colC, colD = st.columns(4)
colA.metric("Market Regime", regime)
colB.metric("Universe Size", f"{universe_size}")
colC.metric("Snapshot Date", f"{latest_usable_date}")
colD.metric("Strategy Cadence", "Weekly (Monday)")

st.markdown("---")

# ------------------------------------------------------------
# Tabs
# ------------------------------------------------------------
tabs = st.tabs([
    "Integrity Panel",
    "Top Signals",
    "Moonshots & Shorts",
    "Prediction Distribution",
    "Model Validation",
    "Strategy Performance",
    "Weekly Audit",
])

df_day_signals = df_day_signals.sort_values("signal_norm", ascending=False)

df_attr_all = report[report["REPORT_SECTION"] == "REPORT_ATTRIBUTION_ROLLING"]
df_attr_day = df_attr_all[df_attr_all["date"].dt.date == latest_usable_date].copy()

pipeline_ok = all([
    integrity["attr_rows_for_latest"],
    integrity["attr_core_non_null"],
    integrity["signals_for_latest"],
    integrity["signals_non_null"],
    integrity["signals_non_zero"],
])

# ------------------------------------------------------------
# TAB 0 — INTEGRITY PANEL
# ------------------------------------------------------------
with tabs[0]:
    st.subheader("Integrity Panel — Pipeline Health Check")
    st.write("Validates that attribution, reporting, and signals are aligned and usable for the latest trading day.")

    def badge(ok: bool) -> str:
        return "🟢 OK" if ok else "🔴 FAIL"

    col1, col2, col3 = st.columns(3)
    col1.write(f"**Signals file loaded:** {badge(integrity['has_signals_file'])}")
    col2.write(f"**Report file loaded:** {badge(integrity['has_report_file'])}")
    col3.write(f"**Latest usable date:** `{latest_usable_date}`")

    col4, col5 = st.columns(2)
    col4.write(f"**Attribution rows for date:** {badge(integrity['attr_rows_for_latest'])}")
    col5.write(f"**Attribution core fields non-null:** {badge(integrity['attr_core_non_null'])}")

    col6, col7, col8 = st.columns(3)
    col6.write(f"**Signals for date:** {badge(integrity['signals_for_latest'])}")
    col7.write(f"**Signals non-null:** {badge(integrity['signals_non_null'])}")
    col8.write(f"**Signals non-zero:** {badge(integrity['signals_non_zero'])}")

    if not pipeline_ok:
        st.error(
            "Pipeline is not healthy for this date. "
            "Re-run:\n\n"
            "1) attribution_engine_v1.py\n"
            "2) reporting_engine_v1.py\n"
            "3) signal_engine_v1_2.py\n"
            "for a recent trading day."
        )
    else:
        st.success("Pipeline is healthy. All other tabs are safe to use.")

# ------------------------------------------------------------
# TAB 1 — TOP SIGNALS
# ------------------------------------------------------------
with tabs[1]:
    st.subheader("Top Signals — Highest Conviction Weekly Opportunities")
    st.write("Strongest signals for the latest usable trading day, normalized within-date.")
    if not pipeline_ok:
        st.warning("Pipeline not healthy — see Integrity Panel. Values may be incomplete.")

    if df_day_signals.empty:
        st.info("No signals available for this date.")
    else:
        df_display = df_day_signals[[
            "ticker",
            "signal_norm",
            "flag",
            "weight_suggestion",
        ]].copy()
        df_display = df_display.round(2)
        st.dataframe(df_display.head(20))

        fig = px.bar(
            df_display.head(10),
            x="ticker",
            y="signal_norm",
            title="Top 10 Signals",
            color="signal_norm",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------
# TAB 2 — MOONSHOTS & SHORTS
# ------------------------------------------------------------
with tabs[2]:
    st.subheader("Moonshots & Shorts — High Conviction Upside and Downside")
    st.write("Moonshots represent high-upside opportunities. Shorts represent high-risk downside names.")
    if not pipeline_ok:
        st.warning("Pipeline not healthy — see Integrity Panel. Values may be incomplete.")

    if df_day_signals.empty:
        st.info("No signals available for this date.")
    else:
        moonshots = df_day_signals[df_day_signals["signal_norm"] > 1.0].round(2)
        shorts = df_day_signals[df_day_signals["signal_norm"] < -1.0].round(2)

        col1, col2 = st.columns(2)
        col1.write("### Moonshots (Upside)")
        col1.dataframe(moonshots[["ticker", "signal_norm", "weight_suggestion"]])

        col2.write("### Shorts (Downside)")
        col2.dataframe(shorts[["ticker", "signal_norm", "weight_suggestion"]])

        if "rolling_volatility" in df_day_signals.columns:
            fig = px.scatter(
                df_day_signals,
                x="rolling_volatility",
                y="signal_norm",
                color="signal_norm",
                title="Signal vs Volatility",
                color_continuous_scale="RdBu"
            )
            st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------
# TAB 3 — PREDICTION DISTRIBUTION
# ------------------------------------------------------------
with tabs[3]:
    st.subheader("Prediction Distribution — Market Signal Shape")
    st.write("Histogram of all signals for the latest usable trading day.")
    if not pipeline_ok:
        st.warning("Pipeline not healthy — see Integrity Panel. Values may be incomplete.")

    if df_day_signals.empty:
        st.info("No signals available for this date.")
    else:
        fig = px.histogram(
            df_day_signals,
            x="signal_norm",
            nbins=40,
            title="Signal Distribution",
            color_discrete_sequence=["#003366"]
        )
        st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------
# TAB 4 — MODEL VALIDATION
# ------------------------------------------------------------
with tabs[4]:
    st.subheader("Model Validation — Confidence & Stability")
    st.write("Validates the signal engine's behavior for the latest usable trading day.")
    if not pipeline_ok:
        st.warning("Pipeline not healthy — see Integrity Panel. Values may be incomplete.")

    if not df_day_signals.empty and "rolling_volatility" in df_day_signals.columns:
        fig = px.scatter(
            df_day_signals,
            x="rolling_volatility",
            y="signal_norm",
            title="Signal vs Volatility",
            color="signal_norm",
            color_continuous_scale="Viridis"
        )
        st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------
# TAB 5 — STRATEGY PERFORMANCE
# ------------------------------------------------------------
with tabs[5]:
    st.subheader("Strategy Performance — 5-Year Backtest")
    st.write("Long-term performance metrics including equity curve and rolling Sharpe (if present in report).")

    if "cumulative_pnl" in report.columns:
        df_equity = report.dropna(subset=["cumulative_pnl"]).copy()
        df_equity = df_equity.sort_values("date")
        df_equity["cumulative_pnl"] = df_equity["cumulative_pnl"].round(2)
        fig = px.line(
            df_equity,
            x="date",
            y="cumulative_pnl",
            title="5-Year Equity Curve",
            color_discrete_sequence=["#003366"]
        )
        st.plotly_chart(fig, use_container_width=True)

    if "rolling_sharpe_60d" in report.columns:
        df_sharpe = report.dropna(subset=["rolling_sharpe_60d"]).copy()
        df_sharpe = df_sharpe.sort_values("date")
        df_sharpe["rolling_sharpe_60d"] = df_sharpe["rolling_sharpe_60d"].round(2)
        fig = px.line(
            df_sharpe,
            x="date",
            y="rolling_sharpe_60d",
            title="60-Day Rolling Sharpe",
            color_discrete_sequence=["#0066CC"]
        )
        st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------
# TAB 6 — WEEKLY AUDIT
# ------------------------------------------------------------
with tabs[6]:
    st.subheader("Weekly Audit — Attribution, Anomalies, Regime Transitions")
    st.write("Quality-control view of attribution and anomalies for the latest usable trading day.")
    if not pipeline_ok:
        st.warning("Pipeline not healthy — see Integrity Panel. Values may be incomplete.")

    if df_attr_day.empty:
        st.info("No attribution rows for this date.")
    else:
        st.write("### Attribution Summary")
        st.dataframe(df_attr_day.round(2))

        st.write("### Anomalies (Z-score > 2)")
        anomaly_cols = [
            "contribution",
            "rolling_beta",
            "rolling_volatility",
        ]
        df_anom = df_attr_day.copy()
        for col in anomaly_cols:
            if col in df_anom.columns:
                std = df_anom[col].std(ddof=0)
                if std and std > 0:
                    df_anom[f"{col}_z"] = (df_anom[col] - df_anom[col].mean()) / std

        z_cols = [c for c in df_anom.columns if c.endswith("_z")]
        if z_cols:
            mask = (df_anom[z_cols].abs() > 2).any(axis=1)
            df_anom = df_anom[mask]
            st.dataframe(df_anom.round(2))
        else:
            st.info("No anomaly metrics available for this date.")