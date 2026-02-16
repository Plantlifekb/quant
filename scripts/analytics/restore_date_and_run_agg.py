#!/usr/bin/env python3
"""
restore_date_and_run_agg.py
- Ensures quant_expected_returns_timeseries.csv has a 'date' column (restores from week_start if needed)
- Runs the repo aggregator
- Prints produced verification files and head of weekly_portfolio_predicted_vs_realized.csv
Save to: scripts/analytics/restore_date_and_run_agg.py
Run from repo root: python .\scripts\analytics\restore_date_and_run_agg.py
"""
import subprocess, sys
from pathlib import Path
import pandas as pd

ROOT = Path.cwd()
exp = ROOT / "data" / "analytics" / "quant_expected_returns_timeseries.csv"
agg_script = ROOT / "scripts" / "analytics" / "aggregate_verification_weekly.py"
outdir = ROOT / "outputs" / "verification"

def ensure_date_column():
    if not exp.exists():
        print("MISSING:", exp)
        return
    df = pd.read_csv(exp)
    # If week_start exists but date does not, restore date from week_start
    if "week_start" in df.columns and "date" not in df.columns:
        df = df.rename(columns={"week_start":"date"})
        # write back with date column present
        df.to_csv(exp, index=False)
        print("Restored 'date' column in", exp)
    elif "date" in df.columns:
        print("'date' column already present in", exp)
    else:
        # try to infer a date-like column and copy it to 'date'
        for col in df.columns:
            try:
                tmp = pd.to_datetime(df[col], errors="coerce")
                if tmp.notna().sum() > 0:
                    df["date"] = tmp.dt.strftime("%Y-%m-%d")
                    df.to_csv(exp, index=False)
                    print(f"Created 'date' column from '{col}' in", exp)
                    return
            except Exception:
                pass
        print("No date-like column found to create 'date' in", exp)

def run_aggregator():
    if not agg_script.exists():
        print("Aggregator script not found:", agg_script)
        return None
    cmd = [sys.executable, str(agg_script)]
    print("Running aggregator:", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc

def print_outputs():
    print("\n---- Produced verification files ----")
    if outdir.exists():
        files = sorted([f for f in outdir.iterdir() if f.is_file()])
        if not files:
            print("No files in", outdir)
        for f in files:
            print(f.name, f.stat().st_size)
    else:
        print("No outputs/verification directory found")

    headfile = outdir / "weekly_portfolio_predicted_vs_realized.csv"
    print("\n---- Head of weekly_portfolio_predicted_vs_realized.csv ----")
    if headfile.exists():
        with headfile.open("r", encoding="utf8") as fh:
            for i, line in enumerate(fh):
                if i >= 40:
                    break
                print(line.rstrip())
    else:
        print("weekly_portfolio_predicted_vs_realized.csv not found")

if __name__ == "__main__":
    ensure_date_column()
    proc = run_aggregator()
    if proc is not None:
        print("\n=== aggregator stdout ===")
        print(proc.stdout.strip())
        print("\n=== aggregator stderr ===")
        print(proc.stderr.strip())
    print_outputs()