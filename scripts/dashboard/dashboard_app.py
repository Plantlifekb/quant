#!/usr/bin/env python3
"""
C:\Quant\scripts\dashboard\dashboard_app.py

Streamlit dashboard for the Quant system — tolerant loader, normalization, validation, audit UI, and charts.
Run:
    streamlit run C:\Quant\scripts\dashboard\dashboard_app.py --server.port 8501
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Dict, Tuple, List
import logging
import traceback

import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# -----------------------
# Configuration / tolerant loader
# -----------------------
BASE = Path(r"C:\Quant")

CANDIDATE_PATHS = {
    "df_perf": [
        BASE / "data" / "analytics" / "weekly_picks_quant_v2.parquet",
        BASE / "data" / "analytics" / "quant_weekly_picks_quant_v1.parquet",
        BASE / "data" / "scripts" / "validation" / "df_perf_export.parquet",
        BASE / "data" / "df_perf.parquet",
    ],
    "weekly_portfolio": [
        BASE / "data" / "analytics" / "strategy_returns.parquet",
        BASE / "data" / "analytics" / "portfolio_performance_quant_v1.parquet",
        BASE / "data" / "analytics" / "performance_quant_v2.parquet",
        BASE / "data" / "weekly_portfolio.parquet",
    ],
    "long_only_weekly": [
        BASE / "data" / "analytics" / "portfolio_performance_quant_v1.parquet",
        BASE / "data" / "analytics" / "strategy_returns.parquet",
        BASE / "data" / "long_only_weekly.parquet",
    ],
    "prices": [
        BASE / "data" / "canonical" / "prices.parquet",
        BASE / "data" / "ingestion" / "prices.parquet",
        BASE / "data" / "analytics" / "price_returns.parquet",
    ],
    "meta": [
        BASE / "data" / "reference" / "securities_master.parquet",
        BASE / "data" / "canonical" / "fundamentals.parquet",
        BASE / "data" / "config" / "ticker_reference.csv",
    ],
}

# -----------------------
# Logging
# -----------------------
_logger = logging.getLogger("quant_dashboard")
if not _logger.handlers:
    _logger.addHandler(logging.StreamHandler())
    _logger.setLevel(logging.INFO)


# -----------------------
# Helpers
# -----------------------
def safe_plotly(fig: go.Figure, key: str):
    try:
        st.plotly_chart(fig, use_container_width=True, key=key)
    except Exception as e:
        st.warning(f"Chart failed to render: {key} • {e}")
        print(f"Chart failed to render: {key}", repr(e))


def safe_last_nonnull(series: pd.Series):
    if series is None:
        return None
    s = series.dropna()
    return s.iloc[-1] if len(s) else None


def pct(x):
    try:
        return f"{x*100:.2f}%"
    except Exception:
        return "n/a"


def safe_render(func, name: str):
    try:
        func()
    except Exception as e:
        st.error(f"{name} failed: {e}")
        print(f"{name} failed:", repr(e))
        print(traceback.format_exc())


# -----------------------
# Loader utilities
# -----------------------
def _try_read_parquet(candidates: List[Path]) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    for p in candidates:
        if p is None:
            continue
        pstr = str(p)
        if p.exists():
            try:
                df = pd.read_parquet(pstr)
                return df, pstr
            except Exception as e:
                print(f"Found file but failed to read: {pstr} • {e}")
                try:
                    st.warning(f"Found file but failed to read: {p.name} • {e}")
                except Exception:
                    pass
    return None, None


def _ensure_datetime(df: Optional[pd.DataFrame], col: str = "date") -> Optional[pd.DataFrame]:
    if df is None or col not in df.columns:
        return df
    try:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    except Exception:
        pass
    return df


# -----------------------
# Read files
# -----------------------
df_perf, df_perf_path = _try_read_parquet(CANDIDATE_PATHS["df_perf"])
weekly_portfolio, wp_path = _try_read_parquet(CANDIDATE_PATHS["weekly_portfolio"])
long_only_weekly, lo_path = _try_read_parquet(CANDIDATE_PATHS["long_only_weekly"])
prices, prices_path = _try_read_parquet(CANDIDATE_PATHS["prices"])
meta, meta_path = _try_read_parquet(CANDIDATE_PATHS["meta"])


# -----------------------
# Canonicalization / Normalization (minimal, auditable)
# -----------------------
# df_perf normalization
if df_perf is not None:
    try:
        df_perf = df_perf.copy()
        df_perf["date"] = pd.to_datetime(df_perf.get("week_start", df_perf.get("date")), errors="coerce")
        df_perf["weight"] = pd.to_numeric(df_perf.get("weight", 0.0), errors="coerce").fillna(0.0)
        df_perf["weekly_realized_return"] = pd.to_numeric(
            df_perf.get("weekly_realized_return", df_perf.get("expected_return", 0.0)),
            errors="coerce",
        ).fillna(0.0)
        df_perf["contribution"] = df_perf["weight"] * df_perf["weekly_realized_return"]
        if "side" not in df_perf.columns:
            df_perf["side"] = df_perf["weight"].apply(lambda x: "long" if x > 0 else ("short" if x < 0 else "flat"))
        df_perf["date"] = df_perf["date"].dt.to_period("W-MON").dt.start_time
    except Exception:
        print("df_perf normalization failed:", traceback.format_exc())

# weekly_portfolio normalization and canonical recompute
if weekly_portfolio is not None:
    try:
        weekly_portfolio = weekly_portfolio.copy()
        weekly_portfolio["date"] = pd.to_datetime(weekly_portfolio["date"], errors="coerce")
        # rename common variants
        if "total_return" in weekly_portfolio.columns and "weekly_return" not in weekly_portfolio.columns:
            weekly_portfolio = weekly_portfolio.rename(columns={"total_return": "weekly_return"})
        if "cum" in weekly_portfolio.columns and "cum_return" not in weekly_portfolio.columns:
            weekly_portfolio = weekly_portfolio.rename(columns={"cum": "cum_return"})
        weekly_portfolio["weekly_return"] = pd.to_numeric(weekly_portfolio.get("weekly_return", 0.0), errors="coerce").fillna(0.0)
        weekly_portfolio = weekly_portfolio.sort_values("date").reset_index(drop=True)
        # canonical cumulative (auditable): compounded product
        weekly_portfolio["cum_return_recomputed"] = (1 + weekly_portfolio["weekly_return"]).cumprod()
    except Exception:
        print("weekly_portfolio normalization failed:", traceback.format_exc())

    # file vs recomputed diff info and anomaly detection (audit)
    try:
        recomputed_last = weekly_portfolio["cum_return_recomputed"].dropna().iloc[-1] if "cum_return_recomputed" in weekly_portfolio.columns else None
    except Exception:
        recomputed_last = None
    file_last = None
    if "cum_return" in weekly_portfolio.columns:
        try:
            file_last = pd.to_numeric(weekly_portfolio["cum_return"], errors="coerce").dropna().iloc[-1] if not weekly_portfolio["cum_return"].dropna().empty else None
        except Exception:
            file_last = None
    elif "cum" in weekly_portfolio.columns:
        try:
            file_last = pd.to_numeric(weekly_portfolio["cum"], errors="coerce").dropna().iloc[-1] if not weekly_portfolio["cum"].dropna().empty else None
        except Exception:
            file_last = None

    diff_info = {"recomputed_last": float(recomputed_last) if recomputed_last is not None else None,
                 "file_last": float(file_last) if file_last is not None else None,
                 "relative_diff": None}
    if recomputed_last is not None and file_last is not None:
        try:
            diff_info["relative_diff"] = abs(file_last - recomputed_last) / max(1.0, abs(recomputed_last))
        except Exception:
            diff_info["relative_diff"] = None

    try:
        st.session_state.setdefault("_loader_diff_info", diff_info)
    except Exception:
        pass

    TOL = 1e-3
    if diff_info.get("relative_diff") is not None and diff_info["relative_diff"] > TOL:
        warning_msg = f"File cum differs from recomputed cum by {diff_info['relative_diff']*100:.2f}% — using recomputed as canonical."
        try:
            st.warning(warning_msg)
        except Exception:
            print(warning_msg)

# long_only normalization
if long_only_weekly is not None:
    try:
        long_only_weekly = long_only_weekly.copy()
        if "portfolio_return" in long_only_weekly.columns and "long_only_return" not in long_only_weekly.columns:
            long_only_weekly = long_only_weekly.rename(columns={"portfolio_return": "long_only_return"})
        if "cumulative_return" in long_only_weekly.columns and "long_only_cum" not in long_only_weekly.columns:
            long_only_weekly = long_only_weekly.rename(columns={"cumulative_return": "long_only_cum"})
        long_only_weekly["date"] = pd.to_datetime(long_only_weekly["date"], errors="coerce").dt.to_period("W-MON").dt.start_time
        long_only_weekly["long_only_return"] = pd.to_numeric(long_only_weekly.get("long_only_return", 0.0), errors="coerce").fillna(0.0)
        long_only_weekly = long_only_weekly.sort_values("date").reset_index(drop=True)
        long_only_weekly["long_only_cum_recomputed"] = (1 + long_only_weekly["long_only_return"]).cumprod()
        if "long_only_cum" not in long_only_weekly.columns or long_only_weekly["long_only_cum"].isnull().all():
            long_only_weekly["long_only_cum"] = long_only_weekly["long_only_cum_recomputed"]
    except Exception:
        print("long_only normalization failed:", traceback.format_exc())

# prices -> market stats (RV20, MA50, MA200) using a proxy ticker
_market_stats = {"rv20": "n/a", "ma50": "n/a", "ma200": "n/a"}
try:
    if prices is not None and not prices.empty:
        prices = prices.copy()
        prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
        proxy = None
        for cand in ("SPX", "^GSPC", "SPY"):
            if cand in prices["ticker"].unique():
                proxy = cand
                break
        if proxy is None:
            proxy = prices["ticker"].value_counts().index[0]
        px = prices.loc[prices["ticker"] == proxy, ["date", "adj_close"]].dropna().set_index("date").sort_index()
        if not px.empty:
            daily_ret = px["adj_close"].pct_change().dropna()
            if len(daily_ret) >= 20:
                _market_stats["rv20"] = f"{(daily_ret.rolling(20).std().iloc[-1]*100):.2f}%"
            if len(px) >= 50:
                _market_stats["ma50"] = f"{px['adj_close'].rolling(50).mean().iloc[-1]:.2f}"
            if len(px) >= 200:
                _market_stats["ma200"] = f"{px['adj_close'].rolling(200).mean().iloc[-1]:.2f}"
except Exception:
    print("market stats computation failed:", traceback.format_exc())


# -----------------------
# Select L/S series (explicit)
# -----------------------
weekly_portfolio_ls = None
try:
    if weekly_portfolio is not None:
        strat_col = "strategy_name" if "strategy_name" in weekly_portfolio.columns else ("strategy" if "strategy" in weekly_portfolio.columns else None)
        if strat_col:
            # prefer explicit LONG_SHORT rows
            mask = weekly_portfolio[strat_col].str.upper() == "LONG_SHORT"
            if mask.any():
                tmp = weekly_portfolio[mask].groupby("date", as_index=False)["weekly_return"].mean().sort_values("date")
                tmp["cum_return"] = (1 + tmp["weekly_return"]).cumprod()
                weekly_portfolio_ls = tmp
        # fallback: use recomputed series from file
        if weekly_portfolio_ls is None:
            if "cum_return_recomputed" in weekly_portfolio.columns:
                weekly_portfolio_ls = weekly_portfolio[["date", "weekly_return", "cum_return_recomputed"]].rename(columns={"cum_return_recomputed": "cum_return"}).copy()
            else:
                weekly_portfolio_ls = weekly_portfolio[["date", "weekly_return"]].copy()
                weekly_portfolio_ls["cum_return"] = (1 + weekly_portfolio_ls["weekly_return"]).cumprod()
except Exception:
    print("Selecting L/S series failed:", traceback.format_exc())


# -----------------------
# Diagnostics helpers
# -----------------------
def show_df_sample(name: str, df: Optional[pd.DataFrame], n: int = 6):
    if df is None:
        st.write(name, "None")
        return
    st.write(name, "shape:", df.shape)
    st.dataframe(df.head(n))
    try:
        st.write("dtypes:", df.dtypes.to_dict())
    except Exception:
        pass
    num = df.select_dtypes(include=["number"])
    if not num.empty:
        st.write("Numeric summary (selected):")
        st.dataframe(num.describe().T[["count", "mean", "std", "min", "max"]])


def run_validation_checks() -> Dict[str, object]:
    checks = {}
    try:
        checks["df_perf_loaded"] = df_perf is not None and not df_perf.empty
        checks["df_perf_contrib_nonzero"] = int((df_perf["contribution"].abs() > 0).sum()) if df_perf is not None and "contribution" in df_perf.columns else 0
        checks["weekly_portfolio_loaded"] = weekly_portfolio is not None and not weekly_portfolio.empty
        checks["weekly_portfolio_ls_loaded"] = weekly_portfolio_ls is not None and not weekly_portfolio_ls.empty
        checks["long_only_loaded"] = long_only_weekly is not None and not long_only_weekly.empty
        # cum consistency
        cum_ok = None
        if weekly_portfolio is not None and "weekly_return" in weekly_portfolio.columns:
            recomputed_last = weekly_portfolio["cum_return_recomputed"].dropna().iloc[-1] if "cum_return_recomputed" in weekly_portfolio.columns else None
            file_last = weekly_portfolio.get("cum_return", None)
            file_last = file_last.dropna().iloc[-1] if (file_last is not None and not file_last.dropna().empty) else None
            if recomputed_last is None:
                cum_ok = False
            elif file_last is None:
                cum_ok = False
            else:
                cum_ok = abs(file_last - recomputed_last) / max(1.0, abs(recomputed_last)) < 1e-3
        checks["weekly_portfolio_cum_consistent"] = bool(cum_ok)
    except Exception as e:
        checks["validation_error"] = str(e)
    return checks


# -----------------------
# Audit report UI
# -----------------------
def audit_report_ui():
    st.markdown("### Audit Report")
    try:
        if weekly_portfolio is None:
            st.write("No weekly_portfolio loaded.")
            return

        st.markdown("**File vs Recomputed cumulative (sample tail)**")
        try:
            cmp = weekly_portfolio[["date"]].drop_duplicates().merge(
                weekly_portfolio[["date", "weekly_return", "cum_return_recomputed"]].drop_duplicates(),
                on="date",
                how="left",
            )
            if "cum_return" in weekly_portfolio.columns:
                file_cum = weekly_portfolio[["date", "cum_return"]].drop_duplicates()
                cmp = cmp.merge(file_cum, on="date", how="left")
            st.dataframe(cmp.tail(12))
        except Exception:
            st.write("Could not build file vs recomputed comparison table.")

        st.markdown("**Recent weekly returns (LONG_SHORT)**")
        try:
            if weekly_portfolio_ls is not None and not weekly_portfolio_ls.empty:
                st.dataframe(weekly_portfolio_ls.sort_values("date").tail(12))
            else:
                st.write("No LONG_SHORT series found; showing recomputed strategy series.")
                tmp = weekly_portfolio[["date", "weekly_return", "cum_return_recomputed"]].drop_duplicates().sort_values("date")
                st.dataframe(tmp.tail(12))
        except Exception:
            st.write("Could not show LONG_SHORT series.")

        st.markdown("**Recent weekly returns (LONG_ONLY)**")
        try:
            if "strategy" in weekly_portfolio.columns:
                lo = weekly_portfolio[weekly_portfolio["strategy"].str.upper() == "LONG_ONLY"].sort_values("date")
                if not lo.empty:
                    st.dataframe(lo[["date", "weekly_return"]].tail(12))
                else:
                    st.write("No LONG_ONLY rows found in strategy file.")
            else:
                st.write("No strategy column present to show LONG_ONLY.")
        except Exception:
            st.write("Could not show LONG_ONLY series.")

        st.markdown("**Validation summary**")
        checks = run_validation_checks()
        st.json(checks)
        # Simple PASS/FAIL message
        ok = checks.get("df_perf_loaded") and checks.get("weekly_portfolio_loaded") and checks.get("df_perf_contrib_nonzero", 0) > 0
        if ok:
            st.success("Basic validation PASS: key inputs loaded and contributions present.")
        else:
            st.error("Basic validation FAIL: inspect diagnostics above.")
    except Exception as e:
        st.write("Audit report generation failed:", e)
        print("Audit report failed:", traceback.format_exc())


# -----------------------
# UI rendering functions
# -----------------------
def compute_summary_metrics(use_file_cum: bool = False, banner_window: int = 4):
    out = {
        "ls_weekly": "n/a",
        "ls_monthly": "n/a",
        "ls_annual": "n/a",
        "lo_weekly": "n/a",
        "lo_monthly": "n/a",
        "lo_annual": "n/a",
        "market_regime_weekly": "N/A",
        "market_current_week": "N/A",
        "rv20": _market_stats.get("rv20", "n/a"),
        "ma50": _market_stats.get("ma50", "n/a"),
        "ma200": _market_stats.get("ma200", "n/a"),
    }

    # L/S metrics: use weekly_portfolio_ls if available, else weekly_portfolio
    ls_source = weekly_portfolio_ls if weekly_portfolio_ls is not None else weekly_portfolio
    if ls_source is not None and "weekly_return" in ls_source.columns:
        try:
            vals = ls_source["weekly_return"].dropna().tail(banner_window)
            out["ls_weekly"] = pct(vals.mean()) if not vals.empty else "n/a"
            out["ls_monthly"] = pct(ls_source["weekly_return"].dropna().tail(4).mean())
            out["ls_annual"] = pct(ls_source["weekly_return"].dropna().mean() * 52)
        except Exception:
            pass

    if long_only_weekly is not None and "long_only_return" in long_only_weekly.columns:
        last_lo = safe_last_nonnull(long_only_weekly["long_only_return"])
        out["lo_weekly"] = pct(last_lo) if last_lo is not None else "n/a"
        try:
            out["lo_monthly"] = pct(long_only_weekly["long_only_return"].dropna().tail(4).mean())
            out["lo_annual"] = pct(long_only_weekly["long_only_return"].dropna().mean() * 52)
        except Exception:
            pass

    return out


def render_banner(metrics: Dict[str, str]):
    st.markdown("## Strategy Performance")
    col1, col2 = st.columns([2, 3])
    with col1:
        st.markdown("**L/S Strategy Performance**")
        st.write(f"**Weekly (4-week avg):** {metrics['ls_weekly']}")
        st.write(f"**Monthly:** {metrics['ls_monthly']}")
        st.write(f"**Annual:** {metrics['ls_annual']}")
    with col2:
        st.markdown("**Long‑Only Performance**")
        st.write(f"**Weekly:** {metrics['lo_weekly']}")
        st.write(f"**Monthly:** {metrics['lo_monthly']}")
        st.write(f"**Annual:** {metrics['lo_annual']}")
    st.markdown("---")
    colA, colB, colC = st.columns([2, 2, 3])
    with colA:
        st.write(f"**Market Regime (Weekly):** {metrics['market_regime_weekly']}")
    with colB:
        st.write(f"**Current-week (market):** {metrics['market_current_week']}")
    with colC:
        st.write(f"**Market RV20:** {metrics['rv20']} • **MA50/MA200:** {metrics['ma50']}/{metrics['ma200']}")


def render_performance_overview(use_file_cum: bool = False):
    st.header("Performance Overview")
    # show file cum vs recomputed in diagnostics area (already available)
    if weekly_portfolio is None or weekly_portfolio.empty:
        st.info("No strategy time-series available.")
    else:
        # Strategy (file)
        if use_file_cum and "cum_return" in weekly_portfolio.columns:
            fig_file = go.Figure()
            fig_file.add_trace(go.Scatter(x=weekly_portfolio["date"], y=weekly_portfolio["cum_return"], mode="lines", name="Strategy (file cum)", line=dict(color="#1f77b4", width=2)))
            fig_file.update_layout(margin=dict(l=20, r=20, t=30, b=20), height=360, template="plotly_white")
            safe_plotly(fig_file, key="perf_overview_file_cum")

        # Recomputed / selected L/S
        if weekly_portfolio_ls is not None and not weekly_portfolio_ls.empty:
            fig_ls = go.Figure()
            fig_ls.add_trace(go.Scatter(x=weekly_portfolio_ls["date"], y=weekly_portfolio_ls["cum_return"], mode="lines", name="L/S (selected)", line=dict(color="#ff7f0e", width=2)))
            fig_ls.update_layout(margin=dict(l=20, r=20, t=30, b=20), height=360, template="plotly_white")
            safe_plotly(fig_ls, key="perf_overview_ls_selected")
        else:
            # fallback to recomputed from file
            if "cum_return_recomputed" in weekly_portfolio.columns:
                fig_re = go.Figure()
                fig_re.add_trace(go.Scatter(x=weekly_portfolio["date"], y=weekly_portfolio["cum_return_recomputed"], mode="lines", name="Strategy (recomputed)", line=dict(color="#ff7f0e", width=2)))
                fig_re.update_layout(margin=dict(l=20, r=20, t=30, b=20), height=360, template="plotly_white")
                safe_plotly(fig_re, key="perf_overview_recomputed")

    # Long-only
    if long_only_weekly is None or long_only_weekly.empty:
        st.info("No long-only history available.")
    else:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=long_only_weekly["date"], y=long_only_weekly["long_only_cum"], mode="lines", name="Long-Only (file)", line=dict(color="#2ca02c", width=2)))
        # also show recomputed if available
        if "long_only_cum_recomputed" in long_only_weekly.columns:
            fig2.add_trace(go.Scatter(x=long_only_weekly["date"], y=long_only_weekly["long_only_cum_recomputed"], mode="lines", name="Long-Only (recomputed)", line=dict(color="#2ca02c", width=1, dash="dash")))
        fig2.update_layout(margin=dict(l=20, r=20, t=30, b=20), height=360, template="plotly_white")
        safe_plotly(fig2, key="perf_overview_lo")


def render_weekly_summary():
    st.header("Weekly Summary — Previous Week")
    if df_perf is None or df_perf.empty or "date" not in df_perf.columns:
        st.info("Row-level picks for previous week are not available.")
        return
    unique_dates = sorted(pd.to_datetime(df_perf["date"].dropna()).unique())
    prev_week_date = unique_dates[-2] if len(unique_dates) >= 2 else (unique_dates[-1] if unique_dates else None)
    st.subheader(f"Summary for week starting: {pd.to_datetime(prev_week_date).date() if prev_week_date is not None else 'n/a'}")
    if prev_week_date is None:
        st.info("Row-level picks for previous week are not available.")
    else:
        week_df = df_perf[pd.to_datetime(df_perf["date"]) == pd.to_datetime(prev_week_date)].copy()
        if not week_df.empty:
            week_df["abs_weight"] = week_df["weight"].abs()
            top10 = week_df.sort_values("abs_weight", ascending=False).head(10)
            display_cols = [c for c in ["ticker", "company_name", "market_sector", "pick_rank", "weight", "side"] if c in top10.columns]
            st.dataframe(top10[display_cols].reset_index(drop=True), use_container_width=True)
        else:
            st.info("No picks for the selected week.")


def render_weekly_picks_latest():
    st.header("Weekly Picks — Latest")
    if df_perf is None or df_perf.empty or "date" not in df_perf.columns:
        st.info("Row-level weekly picks unavailable.")
        return
    latest_date = pd.to_datetime(df_perf["date"].dropna()).max()
    st.subheader(f"As of: {latest_date.date() if not pd.isna(latest_date) else 'n/a'}")
    latest_df = df_perf[pd.to_datetime(df_perf["date"]) == latest_date].copy()
    if latest_df.empty:
        st.info("No picks for the latest week.")
    else:
        display_cols = [c for c in ["ticker", "company_name", "market_sector", "pick_rank", "weight", "side", "contribution"] if c in latest_df.columns]
        st.dataframe(latest_df[display_cols].sort_values(by=["side", "pick_rank"], ascending=[True, True]).reset_index(drop=True), use_container_width=True)


def render_4week_picks_performance():
    st.header("4‑Week Picks Performance")
    if df_perf is None or "date" not in df_perf.columns:
        st.info("Row‑level picks unavailable — cannot compute 4‑week performance.")
        return
    df_perf["date"] = pd.to_datetime(df_perf["date"], errors="coerce")
    recent_weeks = sorted(df_perf["date"].dropna().unique())
    recent_weeks = recent_weeks[-4:] if len(recent_weeks) >= 4 else recent_weeks
    if len(recent_weeks) == 0:
        st.info("Not enough weekly data to compute 4‑week picks.")
        return
    picks_list = []
    top_n = st.selectbox("Top N picks per week", [5, 10, 20, 50, 100], index=1, key="4w_topn")
    side_filter = st.selectbox("Filter by side", ["Long & Short", "Long‑only", "Short‑only"], index=0, key="4w_side")
    for wk in recent_weeks:
        wk_df = df_perf[df_perf["date"] == wk].copy()
        if side_filter == "Long‑only":
            wk_df = wk_df[wk_df["side"].str.lower() == "long"]
        elif side_filter == "Short‑only":
            wk_df = wk_df[wk_df["side"].str.lower() == "short"]
        if wk_df.empty:
            continue
        wk_df["abs_weight"] = wk_df["weight"].abs()
        top = wk_df.sort_values("abs_weight", ascending=False).head(top_n).assign(week=wk)
        picks_list.append(top)
    if not picks_list:
        st.info("No picks available for the selected filters.")
        return
    picks_4w = pd.concat(picks_list, ignore_index=True)
    display_cols = [c for c in ["week", "ticker", "company_name", "market_sector", "pick_rank", "score", "weight", "side", "contribution"] if c in picks_4w.columns]
    st.markdown("### Top Picks (Last 4 Weeks)")
    st.dataframe(picks_4w[display_cols].sort_values(["week", "weight"], ascending=[True, False]), use_container_width=True)

    basket = picks_4w.groupby("week").agg(basket_return=("contribution", "sum")).sort_index()
    basket["cum_basket"] = (1 + basket["basket_return"]).cumprod()
    strat = weekly_portfolio.set_index("date")[["weekly_return"]].rename(columns={"weekly_return": "strategy_return"}) if weekly_portfolio is not None else pd.DataFrame()
    merged = basket.join(strat, how="left")
    merged["cum_strategy"] = (1 + merged["strategy_return"].fillna(0)).cumprod()

    st.markdown("### Basket vs Strategy")
    fig_b = go.Figure()
    fig_b.add_trace(go.Scatter(x=merged.index, y=merged["cum_basket"], mode="lines+markers", name="Top Picks Basket", line=dict(color="#1f77b4", width=3)))
    if "cum_strategy" in merged.columns:
        fig_b.add_trace(go.Scatter(x=merged.index, y=merged["cum_strategy"], mode="lines+markers", name="Overall Strategy", line=dict(color="#ff8c00", width=2)))
    fig_b.update_layout(margin=dict(l=20, r=20, t=30, b=20), height=420, template="plotly_white")
    safe_plotly(fig_b, key="four_week_basket")

    st.markdown("### Contribution Heatmap (Ordered by Total Contribution)")
    heat = picks_4w.pivot_table(index="week", columns="ticker", values="contribution", aggfunc="sum").fillna(0)
    if not heat.empty:
        col_order = heat.sum(axis=0).sort_values(ascending=False).index
        heat = heat[col_order]
        st.dataframe(heat, use_container_width=True)
    else:
        st.info("No contribution heatmap data available.")


def render_contributors_turnover():
    st.header("Contributors & Turnover")
    if df_perf is not None and "ticker" in df_perf.columns:
        ticker_contrib = df_perf.groupby("ticker").agg(total_contribution=("contribution", "sum")).sort_values("total_contribution", ascending=False)
        st.markdown("### Ticker Contributions")
        st.dataframe(ticker_contrib.round(6), use_container_width=True)
    else:
        st.info("No contribution data available.")
    turnover_candidates = [BASE / "data" / "analytics" / "turnover_regime_quant_v1.parquet"]
    turnover_df, _ = _try_read_parquet(turnover_candidates)
    st.markdown("### Turnover (Weekly)")
    if turnover_df is not None and "date" in turnover_df.columns and ("turnover" in turnover_df.columns or "turnover_pct" in turnover_df.columns):
        col = "turnover" if "turnover" in turnover_df.columns else "turnover_pct"
        turnover_df["date"] = pd.to_datetime(turnover_df["date"], errors="coerce")
        fig_to = go.Figure()
        fig_to.add_trace(go.Scatter(x=turnover_df["date"], y=turnover_df[col], mode="lines", line=dict(color="#7f8c8d", width=2)))
        fig_to.update_layout(margin=dict(l=20, r=20, t=30, b=20), height=360, template="plotly_white")
        safe_plotly(fig_to, key="turnover_weekly")
    else:
        st.info("Turnover data not available.")


def render_monthly_summary():
    st.header("Monthly Summary")
    st.info("Monthly summary will show aggregated or row-level month metrics when available.")


def render_long_short_strategy():
    st.header("Combined Long/Short Strategy")
    ls_source = weekly_portfolio_ls if weekly_portfolio_ls is not None else weekly_portfolio
    if ls_source is None or ls_source.empty:
        st.info("No L/S strategy time-series available.")
        return
    fig_ls = go.Figure()
    if "cum_return" not in ls_source.columns:
        ls_source = ls_source.copy()
        ls_source["cum_return"] = (1 + ls_source["weekly_return"].fillna(0)).cumprod()
    fig_ls.add_trace(go.Scatter(x=ls_source["date"], y=ls_source["cum_return"], mode="lines", name="L/S Strategy", line=dict(color="#1f77b4", width=2)))
    fig_ls.update_layout(margin=dict(l=20, r=20, t=30, b=20), height=420, template="plotly_white")
    safe_plotly(fig_ls, key="long_short_strategy")


def render_long_only_strategy():
    st.header("Long‑Only Strategy")
    if long_only_weekly is None or long_only_weekly.empty:
        st.info("No long‑only history available.")
        return
    fig_lo = go.Figure()
    fig_lo.add_trace(go.Scatter(x=long_only_weekly["date"], y=long_only_weekly["long_only_cum"], mode="lines", name="Long‑Only (file)", line=dict(color="#2ca02c", width=2)))
    if "long_only_cum_recomputed" in long_only_weekly.columns:
        fig_lo.add_trace(go.Scatter(x=long_only_weekly["date"], y=long_only_weekly["long_only_cum_recomputed"], mode="lines", name="Long‑Only (recomputed)", line=dict(color="#2ca02c", width=1, dash="dash")))
    fig_lo.update_layout(margin=dict(l=20, r=20, t=30, b=20), height=420, template="plotly_white")
    safe_plotly(fig_lo, key="long_only_strategy")


# -----------------------
# Main app
# -----------------------
def main():
    st.set_page_config(page_title="Quant Dashboard", layout="wide")
    st.title("Quant Dashboard")

    # Diagnostics expander: show samples, dtypes, numeric summaries
    with st.expander("Loader diagnostics (click to expand)"):
        try:
            st.write({
                "df_perf": df_perf_path,
                "weekly_portfolio": wp_path,
                "weekly_portfolio_ls": "derived",
                "long_only_weekly": lo_path,
                "prices": prices_path,
                "meta": meta_path,
            })
            show_df_sample("df_perf", df_perf)
            show_df_sample("weekly_portfolio", weekly_portfolio)
            show_df_sample("weekly_portfolio_ls (L/S candidate)", weekly_portfolio_ls)
            show_df_sample("long_only_weekly", long_only_weekly)
            show_df_sample("prices (sample)", prices.head(20) if prices is not None else None)
            show_df_sample("meta", meta)
            # show file vs recomputed comparison for weekly_portfolio
            if weekly_portfolio is not None:
                try:
                    cmp = weekly_portfolio[["date"]].drop_duplicates().merge(
                        weekly_portfolio[["date", "weekly_return", "cum_return_recomputed"]].drop_duplicates(),
                        on="date",
                        how="left",
                    )
                    if "cum_return" in weekly_portfolio.columns:
                        file_cum = weekly_portfolio[["date", "cum_return"]].drop_duplicates()
                        cmp = cmp.merge(file_cum, on="date", how="left")
                    st.markdown("**Weekly portfolio: file cum vs recomputed (sample)**")
                    st.dataframe(cmp.tail(8))
                except Exception:
                    pass
            # Audit report button
            if st.button("Show Audit Report", key="audit_report"):
                audit_report_ui()
        except Exception as e:
            st.write("Diagnostics failed:", e)
            print("Diagnostics failed:", repr(e))
            print(traceback.format_exc())

    # Validation button
    if st.button("Run validation checks", key="run_validation"):
        checks = run_validation_checks()
        st.json(checks)

    # Toggle: choose whether to display file cum or recomputed cum in charts (dev toggle)
    use_file_cum = st.checkbox("Use file cum where available (dev toggle)", value=False, key="use_file_cum")

    metrics = compute_summary_metrics(use_file_cum=use_file_cum, banner_window=4)
    render_banner(metrics)

    tabs = st.tabs([
        "Performance Overview",
        "Weekly Summary",
        "Weekly Picks",
        "4‑Week Picks Performance",
        "Contributors & Turnover",
        "Monthly Summary",
        "Long/Short Strategy",
        "Long‑Only Strategy",
    ])

    with tabs[0]:
        safe_render(lambda: render_performance_overview(use_file_cum=use_file_cum), "Performance Overview")
    with tabs[1]:
        safe_render(render_weekly_summary, "Weekly Summary")
    with tabs[2]:
        safe_render(render_weekly_picks_latest, "Weekly Picks")
    with tabs[3]:
        safe_render(render_4week_picks_performance, "4-Week Picks Performance")
    with tabs[4]:
        safe_render(render_contributors_turnover, "Contributors & Turnover")
    with tabs[5]:
        safe_render(render_monthly_summary, "Monthly Summary")
    with tabs[6]:
        safe_render(render_long_short_strategy, "Long/Short Strategy")
    with tabs[7]:
        safe_render(render_long_only_strategy, "Long-Only Strategy")

    st.markdown("---")
    data_as_of = None
    if df_perf is not None and "date" in df_perf.columns and not df_perf["date"].dropna().empty:
        data_as_of = pd.to_datetime(df_perf["date"].dropna()).max().date()
    st.write("Data as of: " + (str(data_as_of) if data_as_of else "n/a"))
    st.write(f"Rows — Prices: {len(prices) if prices is not None else 0:,} • Weekly picks rows: {len(df_perf) if df_perf is not None else 0:,} • Ticker meta: {len(meta) if meta is not None else 0:,}")

    if st.button("Refresh data", key="refresh"):
        try:
            params = st.experimental_get_query_params()
            params["_refresh"] = int(pd.Timestamp.utcnow().timestamp())
            st.experimental_set_query_params(**params)
            st.experimental_rerun()
        except Exception:
            st.warning("Refresh requested. Please reload the page manually if automatic refresh fails.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        tb = traceback.format_exc()
        print(tb)
        try:
            st.set_page_config(page_title="Quant Dashboard - Error", layout="wide")
            st.title("Dashboard failed to start")
            st.error(str(e))
            st.code(tb)
        except Exception:
            print("Streamlit UI unavailable to show the error.")
            print(tb)