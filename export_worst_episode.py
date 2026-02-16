# export_worst_episode.py
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# --- CONFIGURE: set path to your CSV if perf is not already in memory ---
csv_path = Path(r"C:\Quant\data\analytics\quant_portfolio_performance_longonly_v2.csv")

# --- load or reuse perf ---
try:
    perf  # if running inside an interactive session where perf exists
    print("Using existing 'perf' variable from environment.")
except NameError:
    if not csv_path.exists():
        print(f"ERROR: CSV not found at {csv_path}. Update csv_path in the script and retry.")
        sys.exit(1)
    perf = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    print(f"Loaded perf from {csv_path}")

# --- ensure pnl series exists and is numeric ---
if 'pnl' not in perf.columns:
    print("ERROR: 'pnl' column not found in perf.")
    sys.exit(1)

perf.index = pd.to_datetime(perf.index)
pnl = perf['pnl'].astype(float).sort_index()

# --- compute NAV and drawdowns ---
cum = (1 + pnl).cumprod()
highs = cum.cummax()
dd = (cum / highs) - 1

# --- detect non-overlapping drawdown episodes ---
episodes = []
i = 0
dates = dd.index
n = len(dates)

while i < n:
    if dd.iloc[i] < 0:
        start_idx = i
        j = start_idx
        while j >= 0 and cum.iloc[j] < highs.iloc[j]:
            j -= 1
        peak_idx = j if j >= 0 else 0

        k = start_idx
        trough_idx = start_idx
        while k < n and dd.iloc[k] < 0:
            if dd.iloc[k] < dd.iloc[trough_idx]:
                trough_idx = k
            k += 1
        recovery_idx = k if k < n else None

        episodes.append({
            "peak": dates[peak_idx],
            "trough": dates[trough_idx],
            "recovery": dates[recovery_idx] if recovery_idx is not None else None,
            "drawdown": float(dd.iloc[trough_idx]),
            "duration_days": (dates[recovery_idx] - dates[peak_idx]).days if recovery_idx is not None else (dates[-1] - dates[peak_idx]).days
        })
        i = k
    else:
        i += 1

episodes_sorted = sorted(episodes, key=lambda x: x["drawdown"])

# --- output ---
out_dir = Path(r"C:\Quant\analysis")
out_dir.mkdir(parents=True, exist_ok=True)

if not episodes_sorted:
    print("No drawdown episodes found (dd never < 0). Nothing written.")
else:
    print("Top 5 worst drawdown episodes:")
    for e in episodes_sorted[:5]:
        peak = e["peak"].date()
        trough = e["trough"].date()
        rec = e["recovery"].date() if e["recovery"] is not None else "N/A"
        print(f"peak {peak}  trough {trough}  recovery {rec}  drawdown {e['drawdown']:.2%}  duration {e['duration_days']}d")

    worst = episodes_sorted[0]
    start = worst["peak"]
    end = worst["recovery"] if worst["recovery"] is not None else dates[-1]
    out_file = out_dir / "worst_episode_pnl.csv"
    pnl.loc[start:end].to_csv(out_file, header=True)
    print(f"\nWrote worst episode pnl to: {out_file}")
    print("\nDaily pnl for worst episode:")
    print(pnl.loc[start:end])