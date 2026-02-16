# C:\Quant\per_day_impact.py
import numpy as np
import pandas as pd
from scipy.stats import mstats
from pathlib import Path
import sys

# CONFIG: path to perf CSV if perf not in session
csv_path = Path(r"C:\Quant\data\analytics\quant_portfolio_performance_longonly_v2.csv")
out_dir = Path(r"C:\Quant\analysis")
out_dir.mkdir(parents=True, exist_ok=True)

# load perf if not present in the interpreter
try:
    perf
except NameError:
    if not csv_path.exists():
        print(f"ERROR: perf not in session and CSV not found at {csv_path}")
        sys.exit(1)
    perf = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    print(f"Loaded perf from {csv_path}")

if 'pnl' not in perf.columns:
    print("ERROR: 'pnl' column not found in perf")
    sys.exit(1)

perf.index = pd.to_datetime(perf.index)
pnl = perf['pnl'].astype(float).sort_index()

# final NAV and per-day impact (vectorized)
final_nav = (1 + pnl).cumprod().iloc[-1]
impacts_s = pd.Series(final_nav * (pnl / (1 + pnl)), index=pnl.index).sort_values()
impacts_file = out_dir / "per_day_impacts.csv"
impacts_s.to_csv(impacts_file, header=["impact_on_final_nav"])
print(f"Wrote per-day impacts to: {impacts_file}\n")

# print top contributors
print("Top negative contributors (largest drag on final NAV):")
print(impacts_s.head(10))
print("\nTop positive contributors:")
print(impacts_s.tail(10))

# identify worst day and recompute metrics without it
worst_day = impacts_s.index[0]
print(f"\nWorst day identified: {worst_day.date()}")

pnl_no = pnl.drop(worst_day, errors='ignore')
nav_no = (1 + pnl_no).cumprod()
rets_no = nav_no.pct_change().dropna()
days_per_year = 252
if len(rets_no) > 0:
    ann_return_no = (1 + rets_no).prod() ** (days_per_year / len(rets_no)) - 1
    ann_vol_no = rets_no.std() * np.sqrt(days_per_year)
    sharpe_no = (rets_no.mean() / rets_no.std()) * np.sqrt(days_per_year) if rets_no.std() > 0 else float('nan')
    print(f"\nWithout {worst_day.date()}: ann_return={ann_return_no:.2%}, vol={ann_vol_no:.2%}, sharpe={sharpe_no:.2f}")
else:
    print("\nNot enough returns after removing worst day to compute metrics.")

# winsorize tails (0.1% each side) and recompute metrics
pnl_w_masked = mstats.winsorize(pnl.values, limits=[0.001, 0.001])
pnl_w = np.ma.filled(pnl_w_masked, fill_value=np.nan)
pnl_w = pd.Series(pnl_w, index=pnl.index).dropna()
nav_w = (1 + pnl_w).cumprod()
rets_w = nav_w.pct_change().dropna()
if len(rets_w) > 0:
    ann_return_w = (1 + rets_w).prod() ** (days_per_year / len(rets_w)) - 1
    ann_vol_w = rets_w.std() * np.sqrt(days_per_year)
    sharpe_w = (rets_w.mean() / rets_w.std()) * np.sqrt(days_per_year) if rets_w.std() > 0 else float('nan')
    print("\nWinsorized (0.1% tails) results:")
    print(f"Ann return: {ann_return_w:.2%}, Ann vol: {ann_vol_w:.2%}, Sharpe: {sharpe_w:.2f}")
else:
    print("\nNot enough returns after winsorize to compute metrics.")