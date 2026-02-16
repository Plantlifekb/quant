# C:\Quant\scripts\analytics\build_strategy_returns_from_attr.py
import pandas as pd
from pathlib import Path
import sys

ATTR = Path("C:/Quant/data/analytics/attribution_quant_v2.parquet")
OUT = Path("C:/Quant/data/analytics/strategy_returns.parquet")

if not ATTR.exists():
    print("ERROR: attribution file not found:", ATTR)
    sys.exit(1)

df = pd.read_parquet(ATTR)
df['date'] = pd.to_datetime(df['date'], errors='coerce')

# Build a per-row contribution column if not present
if {'factor_contrib','alpha_contrib','idio_contrib'}.issubset(df.columns):
    df['contrib'] = df['factor_contrib'].fillna(0.0) + df['alpha_contrib'].fillna(0.0) + df['idio_contrib'].fillna(0.0)
else:
    # fallback to total_return if contrib columns are not available
    if 'total_return' in df.columns:
        df['contrib'] = df['total_return'].fillna(0.0)
    else:
        print("ERROR: attribution lacks contribution and total_return columns.")
        sys.exit(2)

# Aggregate positive contributions as LONG_ONLY, negative as LONG_SHORT
long = df[df['contrib'] > 0].groupby('date')['contrib'].sum().reset_index().rename(columns={'contrib':'total_return'})
long['strategy'] = 'LONG_ONLY'
short = df[df['contrib'] < 0].groupby('date')['contrib'].sum().reset_index().rename(columns={'contrib':'total_return'})
short['strategy'] = 'LONG_SHORT'

out = pd.concat([long, short], ignore_index=True).sort_values(['strategy','date']).reset_index(drop=True)
out.to_parquet(OUT, index=False)
print("WROTE:", OUT, "rows:", len(out))

# Diagnostics
for strat in ['LONG_ONLY','LONG_SHORT']:
    sub = out[out['strategy']==strat]
    if sub.empty:
        print(f"{strat}: NO ROWS")
        continue
    rows = len(sub)
    nonzero = int((sub['total_return'] != 0.0).sum())
    date_min = sub['date'].min()
    date_max = sub['date'].max()
    cum_latest = (1 + sub['total_return']).cumprod().iloc[-1] - 1
    print(f"{strat}: rows={rows}, nonzero_days={nonzero}, date_range={date_min} -> {date_max}, cum_latest={cum_latest:.6f}")
    print(sub.head(10).to_string(index=False))