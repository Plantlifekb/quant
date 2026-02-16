#!/usr/bin/env python3
"""
C:\Quant\scripts\dashboard\dashboard_app.py

Compute and dashboard helper functions used by tests and the pipeline.

Key behaviors:
- Respects DATA_ROOT environment variable (defaults to C:\Quant\data)
- Robust CSV/parquet reading with encoding fallbacks
- Aggregates event-level realized returns into daily realized_return
- Normalizes tickers to _tk and dates to naive midnight
- Persists merged_attribution.parquet and writes audit entries
- Returns deterministic, test-friendly DataFrames
"""
from __future__ import annotations

import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd
import numpy as np

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def append_audit_log(message: str) -> None:
    try:
        analytics = Path(os.environ.get("DATA_ROOT", r"C:\Quant\data")) / "analytics"
        analytics.mkdir(parents=True, exist_ok=True)
        with open(analytics / "perf_audit.log", "a", encoding="utf-8") as f:
            f.write(f"{datetime.utcnow().isoformat()}Z\t{message}\n")
    except Exception:
        # never raise from audit logging
        pass

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def normalize_ticker_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.upper().str.strip()
    s = s.str.split(r'\.').str[0]
    s = s.str.replace(r'^EQ-', '', regex=True)
    return s

def find_col(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def safe_read_parquet(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        traceback.print_exc()
        return pd.DataFrame()

def safe_read_csv(path: str, **kwargs) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8", **kwargs)
    except UnicodeDecodeError:
        try:
            return pd.read_csv(path, encoding="latin-1", **kwargs)
        except Exception:
            try:
                return pd.read_csv(path, encoding="utf-8", errors="replace", **kwargs)
            except Exception:
                traceback.print_exc()
                return pd.DataFrame()
    except Exception:
        traceback.print_exc()
        return pd.DataFrame()

def detect_unit_and_convert(series: pd.Series, name: str = "") -> Tuple[pd.Series, str]:
    s = pd.to_numeric(series, errors="coerce").abs().dropna()
    if s.empty:
        return series.astype(float), "unknown"
    med = s.median()
    if med > 100.0:
        converted = series.astype(float) / 10000.0
        decision = "bps->decimal"
    elif med > 1.0:
        converted = series.astype(float) / 100.0
        decision = "percent->decimal"
    else:
        converted = series.astype(float)
        decision = "decimal"
    append_audit_log(f"unit_detect name={name} median_abs={med:.6g} decision={decision}")
    return converted, decision

# -----------------------------------------------------------------------------
# Main compute function
# -----------------------------------------------------------------------------

def compute_attribution_production(
    weight_col_choice: str | None = None,
    normalize_weights_flag: bool = False,
    calendar_freq: str = "B",
    rebuild_weights: bool = False
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """
    Compute attribution production artifacts.

    Returns:
      daily, top, merged, diag
    """
    diag = {}
    try:
        DATA_ROOT = os.environ.get("DATA_ROOT", r"C:\Quant\data")
        ANALYTICS_PATH = Path(DATA_ROOT) / "analytics"
        ANALYTICS_PATH.mkdir(parents=True, exist_ok=True)

        realized_path = ANALYTICS_PATH / "realized_returns.parquet"
        weights_csv_path = ANALYTICS_PATH / "quant_portfolio_weights_ensemble_risk_longshort_v2_trading.csv"
        w_daily_path = ANALYTICS_PATH / "w_daily.parquet"
        merged_out_path = ANALYTICS_PATH / "merged_attribution.parquet"

        # Read realized returns
        realized = safe_read_parquet(str(realized_path))
        realized = _normalize_columns(realized)
        diag["realized_rows"] = int(len(realized))

        if realized.empty:
            diag["error"] = "Missing realized_returns input"
            append_audit_log("compute_attribution_production: missing realized_returns")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), diag

        # Robust column detection
        date_col = find_col(realized, ["date", "trade_date", "dt"])
        ticker_col_r = find_col(realized, ["ticker", "symbol", "asset"])
        ret_col = find_col(realized, ["realized_return", "return", "ret", "pnl", "pnl_pct"])

        if date_col is None or ticker_col_r is None or ret_col is None:
            diag["error"] = "Missing required columns in returns"
            append_audit_log("compute_attribution_production: missing required columns in realized")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), diag

        # Normalize realized events
        realized[date_col] = pd.to_datetime(realized[date_col], errors="coerce")
        realized["_tk"] = normalize_ticker_series(realized[ticker_col_r])

        realized[ret_col], ret_unit_decision = detect_unit_and_convert(realized[ret_col], name="realized_return")
        diag["ret_unit_decision"] = ret_unit_decision

        # Aggregate event-level rows into daily per (date, _tk)
        realized_agg = (
            realized.groupby([date_col, "_tk"], as_index=False)[ret_col]
            .sum()
            .rename(columns={ret_col: "realized_return"})
        )
        # Ensure date column is named 'date' and normalized
        if date_col != "date":
            realized_agg = realized_agg.rename(columns={date_col: "date"})
        realized_agg["date"] = pd.to_datetime(realized_agg["date"], errors="coerce").dt.normalize()

        # Load w_daily or fallback to CSV snapshot or empty
        if w_daily_path.exists():
            w_daily = safe_read_parquet(str(w_daily_path))
            w_daily = _normalize_columns(w_daily)
            if "date" in w_daily.columns:
                w_daily["date"] = pd.to_datetime(w_daily["date"], errors="coerce")
            if "_tk" not in w_daily.columns and "ticker" in w_daily.columns:
                w_daily["_tk"] = normalize_ticker_series(w_daily["ticker"])
        else:
            if Path(weights_csv_path).exists():
                w_daily = safe_read_csv(str(weights_csv_path))
                w_daily = _normalize_columns(w_daily)
                if "date" in w_daily.columns:
                    w_daily["date"] = pd.to_datetime(w_daily["date"], errors="coerce")
                if "_tk" not in w_daily.columns and "ticker" in w_daily.columns:
                    w_daily["_tk"] = normalize_ticker_series(w_daily["ticker"])
            else:
                w_daily = pd.DataFrame(columns=["date", "_tk", "_weight_used"])

        # Determine weight column
        if "_weight_used" not in w_daily.columns:
            for cand in ["weight", "weight_trading_v2", "w", "position_weight", "target_weight"]:
                if cand in w_daily.columns:
                    w_daily["_weight_used"] = pd.to_numeric(w_daily[cand], errors="coerce").fillna(0.0)
                    break
            if "_weight_used" not in w_daily.columns:
                w_daily["_weight_used"] = 0.0

        # Merge aggregated realized returns with weights
        merged = pd.merge(realized_agg, w_daily, how="left", left_on=["date", "_tk"], right_on=["date", "_tk"])

        matched = int(merged[~merged["_weight_used"].isna()].shape[0]) if "_weight_used" in merged.columns else 0
        total = int(merged.shape[0])
        missing_weights = int(merged["_weight_used"].isna().sum()) if "_weight_used" in merged.columns else total
        diag["matched_rows"] = matched
        diag["total_rows_after_merge"] = total
        diag["missing_weights_after_merge"] = missing_weights

        merged["_weight_used"] = pd.to_numeric(merged.get("_weight_used", 0.0)).fillna(0.0).astype(float)
        merged["realized_return"] = pd.to_numeric(merged["realized_return"]).fillna(0.0).astype(float)
        merged["contrib_total"] = merged["_weight_used"] * merged["realized_return"]

        # Persist merged attribution
        try:
            merged.to_parquet(str(merged_out_path), index=False)
            append_audit_log(f"persisted merged_attribution rows={len(merged)}")
        except Exception:
            append_audit_log("failed to persist merged_attribution")

        # Build daily and top summaries
        daily = merged.groupby("date")["contrib_total"].sum().reset_index().rename(columns={"contrib_total": "total_contribution"})
        merged["abs_total_contrib"] = merged["contrib_total"].abs()
        top = merged.groupby("_tk")["abs_total_contrib"].sum().reset_index().rename(columns={"abs_total_contrib": "abs_total_contrib"}).sort_values("abs_total_contrib", ascending=False).head(100)

        # Try to enrich top with metadata
        meta = safe_read_csv(str(ANALYTICS_PATH / "company_metadata.csv"))
        if meta.empty:
            sample_picks = ANALYTICS_PATH / "quant_weekly_picks_quant_v1.parquet"
            if sample_picks.exists():
                try:
                    picks = pd.read_parquet(sample_picks)
                    picks = _normalize_columns(picks)
                    if "company_name" in picks.columns and "sector" in picks.columns and "ticker" in picks.columns:
                        meta = picks[["ticker", "company_name", "sector"]].drop_duplicates(subset=["ticker"]).rename(columns={"ticker": "ticker"})
                except Exception:
                    pass

        if not meta.empty:
            meta = _normalize_columns(meta)
            if "ticker" in meta.columns:
                meta["_tk"] = normalize_ticker_series(meta["ticker"])
            else:
                meta["_tk"] = meta.index.astype(str)
            meta = meta[["_tk"] + [c for c in meta.columns if c not in ["ticker", "_tk"]]]
        else:
            meta = pd.DataFrame(columns=["_tk", "company_name", "sector"])

        weight_stats = merged.groupby("_tk")["_weight_used"].agg(avg_weight="mean", avg_abs_weight=lambda x: x.abs().mean()).reset_index()
        top = top.merge(weight_stats, on="_tk", how="left")
        top = top.merge(meta, on="_tk", how="left")
        cols = ["_tk", "company_name", "sector", "abs_total_contrib", "avg_weight", "avg_abs_weight"]
        for c in cols:
            if c not in top.columns:
                top[c] = None
        top = top[cols].rename(columns={"_tk": "ticker"})

        # Provide diagnostics about which columns were used
        W_DAILY_INFO = {"weight_col": "_weight_used"}
        REALIZED_INFO = {"return_col": "realized_return"}
        diag["weight_col_used"] = W_DAILY_INFO.get("weight_col")
        diag["ret_col_used"] = REALIZED_INFO.get("return_col")

        # --- Final deterministic reconciliation to guarantee one row per (date, _tk) with aggregated realized_return ---
        try:
            # Recompute aggregated realized returns from any available realized-like columns
            if "realized_return" in merged.columns:
                realized_source = merged[["date", "_tk", "realized_return"]].copy()
            else:
                cand = None
                for c in ["realized_return", "return", "ret", "pnl", "pnl_pct"]:
                    if c in merged.columns:
                        cand = c
                        break
                if cand is not None:
                    realized_source = merged[["date", "_tk", cand]].rename(columns={cand: "realized_return"}).copy()
                else:
                    realized_source = pd.DataFrame(columns=["date", "_tk", "realized_return"])

            if not realized_source.empty:
                realized_source["date"] = pd.to_datetime(realized_source["date"], errors="coerce").dt.normalize()
                realized_source["_tk"] = normalize_ticker_series(realized_source["_tk"].astype(str))
                realized_agg = realized_source.groupby(["date", "_tk"], as_index=False)["realized_return"].sum()
            else:
                realized_agg = pd.DataFrame(columns=["date", "_tk", "realized_return"])

            # Ensure merged has date/_tk columns to join on
            merged = merged.copy()
            if "date" in merged.columns:
                merged["date"] = pd.to_datetime(merged["date"], errors="coerce").dt.normalize()
            else:
                merged["date"] = pd.NaT
            if "_tk" in merged.columns:
                merged["_tk"] = normalize_ticker_series(merged["_tk"].astype(str))
            elif "ticker" in merged.columns:
                merged["_tk"] = normalize_ticker_series(merged["ticker"].astype(str))
            else:
                merged["_tk"] = ""

            # Left-join aggregated realized back onto merged to guarantee presence and correct values
            merged = pd.merge(realized_agg, merged.drop(columns=["realized_return"], errors="ignore"), how="left", on=["date", "_tk"])

            # Fill missing numeric fields safely
            merged["realized_return"] = pd.to_numeric(merged.get("realized_return", 0.0), errors="coerce").fillna(0.0)
            merged["_weight_used"] = pd.to_numeric(merged.get("_weight_used", 0.0), errors="coerce").fillna(0.0)
            merged["contrib_total"] = merged["_weight_used"] * merged["realized_return"]

            # Reset index and ensure deterministic ordering
            merged = merged.reset_index(drop=True)
        except Exception:
            try:
                merged["date"] = pd.to_datetime(merged.get("date"), errors="coerce").dt.normalize()
                merged["_tk"] = normalize_ticker_series(merged.get("_tk", merged.get("ticker", "")).astype(str))
                merged["realized_return"] = pd.to_numeric(merged.get("realized_return", 0.0), errors="coerce").fillna(0.0)
                merged = merged.reset_index(drop=True)
            except Exception:
                pass

        return daily, top, merged, diag

    except Exception:
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {"error": "exception during compute"}