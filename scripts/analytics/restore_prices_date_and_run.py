#!/usr/bin/env python3
# restore_prices_date_and_run.py
# Restores a 'date' column in quant_prices_v1.csv if needed, then runs the aggregator
import subprocess, sys
from pathlib import Path
import pandas as pd

ROOT = Path.cwd()
prices = ROOT / "data" / "analytics" / "quant_prices_v1.csv"
agg = ROOT / "scripts" / "analytics" / "aggregate_verification_weekly.py"
outdir = ROOT / "outputs" / "verification"

# restore date column if week_start exists
if prices.exists():
    df = pd.read_csv(prices)
    if "week_start" in df.columns and "date" not in df.columns:
        df = df.rename(columns={"week_start": "date"})
        df.to_csv(prices, index=False)
        print("Restored 'date' column in", prices)
    elif "date" in df.columns:
        print("'date' column already present in", prices)
    else:
        print("No 'week_start' or 'date' column found in", prices)
else:
    print("Missing prices file:", prices)
    sys.exit(1)

# run aggregator
cmd = [sys.executable, str(agg)]
print("Running aggregator:", " ".join(cmd))
proc = subprocess.run(cmd, capture_output=True, text=True)
print("\n=== aggregator stdout ===")
print(proc.stdout.strip())
print("\n=== aggregator stderr ===")
print(proc.stderr.strip())

# list outputs and print head of weekly aggregate
print("\n---- Produced verification files ----")
if outdir.exists():
    for f in sorted(outdir.glob("*")):
        if f.is_file():
            print(f.name, f.stat().st_size)
else:
    print("No outputs/verification directory found")

head = outdir / "weekly_portfolio_predicted_vs_realized.csv"
print("\n---- Head of weekly_portfolio_predicted_vs_realized.csv ----")
if head.exists():
    with head.open("r", encoding="utf8") as fh:
        for i, line in enumerate(fh):
            if i >= 40:
                break
            print(line.rstrip())
else:
    print("weekly_portfolio_predicted_vs_realized.csv not found")