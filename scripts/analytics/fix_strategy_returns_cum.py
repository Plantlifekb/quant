# C:\Quant\scripts\analytics\fix_strategy_returns_cum.py
import pandas as pd, numpy as np
from pathlib import Path
p = Path("C:/Quant/data/analytics/strategy_returns.parquet")
out = Path("C:/Quant/data/analytics/strategy_returns_fixed.parquet")
df = pd.read_parquet(p)
df['total_return'] = pd.to_numeric(df['total_return'], errors='coerce').fillna(0.0)

# Convert percent->decimal if many values > 1.0
if (df['total_return'].abs() > 1.0).mean() > 0.05:
    print("Converting returns by dividing by 100 (detected many >1.0 values).")
    df['total_return'] = df['total_return'] / 100.0

fixed = []
for strat in df['strategy'].unique():
    sub = df[df['strategy']==strat].sort_values('date').set_index('date').copy()
    sub['log1p'] = np.log1p(sub['total_return'].clip(lower=-0.999999))
    sub['cum'] = np.expm1(sub['log1p'].cumsum())
    fixed.append(sub.reset_index()[['date','strategy','total_return','cum']])
out_df = pd.concat(fixed, ignore_index=True).sort_values(['strategy','date'])
out_df.to_parquet(out, index=False)
print("Wrote fixed strategy returns to", out)
print(out_df.groupby('strategy')['cum'].last())