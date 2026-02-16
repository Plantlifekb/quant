# C:\Quant\scripts\analytics\auto_fix_strategy_returns.py
import pandas as pd, numpy as np
from pathlib import Path
p = Path("C:/Quant/data/analytics/strategy_returns.parquet")
out_fixed = Path("C:/Quant/data/analytics/strategy_returns_fixed.parquet")
out_overwrite = Path("C:/Quant/data/analytics/strategy_returns.parquet")  # will overwrite after creating fixed

if not p.exists():
    print("ERROR: strategy_returns.parquet not found at", p)
    raise SystemExit(1)

df = pd.read_parquet(p)
df['total_return'] = pd.to_numeric(df['total_return'], errors='coerce').fillna(0.0)

# Detect percent units and convert if many values > 1.0
if (df['total_return'].abs() > 1.0).mean() > 0.05:
    print("Detected many values > 1.0 — converting percent->decimal by dividing by 100.")
    df['total_return'] = df['total_return'] / 100.0

# Clip extreme daily returns to +/-50% to remove data errors
clip_val = 0.5
df['total_return_clipped'] = df['total_return'].clip(lower=-clip_val, upper=clip_val)

# Compute numerically stable cumulative returns per strategy using log1p
fixed_parts = []
for strat in sorted(df['strategy'].unique()):
    sub = df[df['strategy']==strat].sort_values('date').copy()
    sub['log1p'] = np.log1p(sub['total_return_clipped'].clip(lower=-0.999999))
    sub['cum'] = np.expm1(sub['log1p'].cumsum())
    fixed_parts.append(sub[['date','strategy','total_return_clipped','cum']].rename(columns={'total_return_clipped':'total_return'}))

out_df = pd.concat(fixed_parts, ignore_index=True).sort_values(['strategy','date'])
out_df.to_parquet(out_fixed, index=False)
# Overwrite original only after fixed file is written
out_df.to_parquet(out_overwrite, index=False)
print("WROTE fixed file to", out_fixed)
print("Overwrote original at", out_overwrite)
for strat in out_df['strategy'].unique():
    last = out_df[out_df['strategy']==strat].sort_values('date')['cum'].iloc[-1]
    print(f"{strat} final cum: {last:.6f}")