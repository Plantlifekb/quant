#!/usr/bin/env python3
"""
recompute_perf_safe.py
"""
import argparse
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

DATA_ROOT = os.environ.get("DATA_ROOT", r"C:\Quant\data")
ANALYTICS_PATH = os.path.join(DATA_ROOT, "analytics")
MERGED_ATTR_PQ = os.path.join(ANALYTICS_PATH, "merged_attribution.parquet")
PERF_WEEKLY_PQ = os.path.join(ANALYTICS_PATH, "perf_weekly.parquet")
AUDIT_LOG = os.path.join(ANALYTICS_PATH, "perf_audit.log")
os.makedirs(ANALYTICS_PATH, exist_ok=True)

def append_audit_log(message: str):
    try:
        with open(AUDIT_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.utcnow().isoformat()}Z\t{message}\n")
    except Exception:
        pass

def safe_read_parquet(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()

def compute_performance_safe(merged_path: str, out_path: str, freq: str = "W", persist: bool = True):
    merged = safe_read_parquet(merged_path)
    if merged.empty:
        append_audit_log(f"compute_performance_safe: missing merged_attribution at {merged_path}")
        return pd.DataFrame()
    if "date" not in merged.columns:
        append_audit_log("compute_performance_safe: merged missing 'date' column")
        return pd.DataFrame()
    merged = merged.copy()
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce")
    if "contrib_total" not in merged.columns:
        if "_weight_used" in merged.columns and "realized_return" in merged.columns:
            merged["contrib_total"] = merged["_weight_used"].astype(float) * merged["realized_return"].astype(float)
        else:
            append_audit_log("compute_performance_safe: merged missing 'contrib_total' and cannot derive it")
            return pd.DataFrame()
    daily_portfolio = merged.groupby("date", as_index=False)["contrib_total"].sum().rename(columns={"contrib_total": "daily_portfolio_return"})
    perf_series = daily_portfolio.set_index("date")["daily_portfolio_return"].resample(freq).sum()
    perf = perf_series.to_frame(name="period_return").reset_index()
    perf["period_return_pct"] = perf["period_return"] * 100.0
    perf["flag_suspicious"] = perf["period_return_pct"].abs() > 10000.0
    perf["flag_count"] = perf["flag_suspicious"].astype(int)
    perf["unit_decision"] = "decimal"
    total_flagged = int(perf["flag_suspicious"].sum())
    try:
        if persist:
            perf.to_parquet(out_path, index=False)
            append_audit_log(f"persisted perf freq={freq} rows={len(perf)} flagged_periods={total_flagged} unit_decision=decimal")
    except Exception as e:
        append_audit_log(f"failed to persist perf_weekly error={str(e)}")
    return perf

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--freq", default="W")
    parser.add_argument("--merged", default=MERGED_ATTR_PQ)
    parser.add_argument("--out", default=PERF_WEEKLY_PQ)
    args = parser.parse_args()
    perf = compute_performance_safe(args.merged, args.out, freq=args.freq, persist=True)
    if perf.empty:
        print("No perf produced. Check merged_attribution.parquet and audit log.")
        return
    rows = len(perf)
    flagged = int(perf["flag_suspicious"].sum()) if "flag_suspicious" in perf.columns else 0
    max_pct = float(perf["period_return_pct"].abs().max()) if "period_return_pct" in perf.columns else np.nan
    print("Perf recompute complete")
    print("  rows:", rows)
    print("  flagged periods (flag_suspicious):", flagged)
    print("  max abs period_return_pct:", max_pct)
    append_audit_log(f"recompute_perf_safe completed rows={rows} flagged_periods={flagged} max_abs_pct={max_pct}")

if __name__ == "__main__":
    main()