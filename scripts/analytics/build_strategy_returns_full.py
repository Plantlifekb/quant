# C:\Quant\scripts\analytics\build_strategy_returns_full.py
import pandas as pd, numpy as np, json
from pathlib import Path
A = Path("C:/Quant/data/analytics/attribution_quant_v2.parquet")
OUT = Path("C:/Quant/data/analytics/strategy_returns.parquet")
MAN = Path("C:/Quant/data/analytics/strategy_returns_manifest.json")

if not A.exists():
    raise SystemExit(f"Missing attribution file: {A}")

df = pd.read_parquet(A)
df['date'] = pd.to_datetime(df['date'], errors='coerce')

if {'factor_contrib','alpha_contrib','idio_contrib'}.issubset(df.columns):
    df['contrib'] = df['factor_contrib'].fillna(0.0) + df['alpha_contrib'].fillna(0.0) + df['idio_contrib'].fillna(0.0)
elif 'total_return' in df.columns:
    df['contrib'] = df['total_return'].fillna(0.0)
else:
    raise SystemExit("Attribution lacks contribution and total_return columns")

if 'weight' in df.columns:
    expo = df.groupby('date')['weight'].apply(lambda s: s.abs().sum()).rename('total_abs_exposure').reset_index()
    df = df.merge(expo, on='date', how='left')
    df['return_est'] = df.apply(
        lambda r: (r['contrib'] / r['total_abs_exposure']) if pd.notna(r['total_abs_exposure']) and r['total_abs_exposure'] != 0 else r['contrib'],
        axis=1
    )
else:
    df['return_est'] = df['contrib']

long = df[df['return_est'] > 0].groupby('date')['return_est'].sum().reset_index().rename(columns={'return_est':'total_return'})
long['strategy'] = 'LONG_ONLY'
short = df[df['return_est'] < 0].groupby('date')['return_est'].sum().reset_index().rename(columns={'return_est':'total_return'})
short['strategy'] = 'LONG_SHORT'
out = pd.concat([long, short], ignore_index=True).sort_values(['strategy','date']).reset_index(drop=True)

if (out['total_return'].abs() > 1.0).mean() > 0.05:
    out['total_return'] = out['total_return'] / 100.0
    unit_converted = True
else:
    unit_converted = False

clip_val = 0.5
out['total_return_clipped'] = out['total_return'].clip(lower=-clip_val, upper=clip_val)

parts = []
for strat in sorted(out['strategy'].unique()):
    sub = out[out['strategy']==strat].sort_values('date').set_index('date').copy()
    sub['log1p'] = np.log1p(sub['total_return_clipped'].clip(lower=-0.999999))
    sub['cum'] = np.expm1(sub['log1p'].cumsum())
    parts.append(sub.reset_index()[['date','strategy','total_return_clipped','cum']].rename(columns={'total_return_clipped':'total_return'}))
final = pd.concat(parts, ignore_index=True).sort_values(['strategy','date'])
final.to_parquet(OUT, index=False)

manifest = {
    "rows": len(final),
    "unit_converted": unit_converted,
    "clip_value": clip_val,
    "date_min": str(final['date'].min()),
    "date_max": str(final['date'].max()),
    "long_rows": int((final['strategy']=='LONG_ONLY').sum()),
    "short_rows": int((final['strategy']=='LONG_SHORT').sum())
}
MAN.write_text(json.dumps(manifest, indent=2))
print("WROTE", OUT, "and manifest", MAN)