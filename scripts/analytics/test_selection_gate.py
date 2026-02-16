#!/usr/bin/env python3
# scripts/analytics/test_selection_gate.py
"""
Test selection gate across historical weekly predicted picks.
Run:
    python .\scripts\analytics\test_selection_gate.py
"""
from pathlib import Path
import pandas as pd

ROOT = Path.cwd()
OUT_VERIF = ROOT / "outputs" / "verification"
PICK_FILE = OUT_VERIF / "predicted_vs_picks_weekly_longshort.csv"

MIN_EXPECTED_GAIN = 0.001
REGIME_MIN = -0.0005

def load_picks(path):
    if not path.exists():
        print("Pick history not found:", path)
        return None
    df = pd.read_csv(path)
    # tolerant mapping
    if "predicted_week_gain" not in df.columns and "predicted_gain" in df.columns:
        df = df.rename(columns={"predicted_gain":"predicted_week_gain"})
    if "target_weight" not in df.columns and "weight" in df.columns:
        df = df.rename(columns={"weight":"target_weight"})
    if "week_start" not in df.columns and "date" in df.columns:
        df = df.rename(columns={"date":"week_start"})
    # coerce
    df["predicted_week_gain"] = pd.to_numeric(df.get("predicted_week_gain", 0), errors="coerce").fillna(0.0)
    df["target_weight"] = pd.to_numeric(df.get("target_weight", 0), errors="coerce").fillna(0.0)
    if "week_start" in df.columns:
        df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")
    return df

def main():
    df = load_picks(PICK_FILE)
    if df is None:
        return

    # Group safely using an explicit aggregation to avoid deprecated apply behavior
    if "week_start" in df.columns:
        grouped = df.groupby("week_start").agg(expected_gain=pd.NamedAgg(column="predicted_week_gain", aggfunc=lambda s: (s * df.loc[s.index, "target_weight"]).sum()))
        grouped = grouped.reset_index()
    else:
        expected_gain = (df["target_weight"] * df["predicted_week_gain"]).sum()
        grouped = pd.DataFrame([{"week_start": pd.NaT, "expected_gain": expected_gain}])

    # regime_score placeholder (0.0). If you have a regime timeseries, merge here.
    grouped["regime_score"] = 0.0
    grouped["gated"] = (grouped["expected_gain"] < MIN_EXPECTED_GAIN) | (grouped["regime_score"] < REGIME_MIN)

    total = len(grouped)
    n_gated = int(grouped["gated"].sum())
    pct_gated = n_gated / total if total else 0.0

    print("Selection gate test")
    print("-------------------")
    print(f"Pick file: {PICK_FILE}")
    print(f"Total periods: {total}")
    print(f"Gated periods: {n_gated} ({pct_gated:.2%})")
    print()
    if n_gated:
        print("Sample gated periods (most recent 20):")
        print(grouped[grouped["gated"]].sort_values("week_start", ascending=False).head(20).to_string(index=False))
    else:
        print("No gated periods under current thresholds.")

if __name__ == "__main__":
    main()