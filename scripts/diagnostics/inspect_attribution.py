# C:\Quant\scripts\diagnostics\inspect_attribution.py
import sys
from pathlib import Path
import pandas as pd

p = Path("C:/Quant/data/analytics/attribution_quant_v2.parquet")
if not p.exists():
    print("Attribution file not found:", p)
    sys.exit(1)

df = pd.read_parquet(p)
print("Rows:", len(df))
print("Columns:", list(df.columns))
if "strategy" in df.columns:
    svals = sorted(df['strategy'].dropna().astype(str).unique())
    print("Unique strategy values:", svals)
    print("Strategy counts (top 20):")
    print(df['strategy'].astype(str).value_counts(dropna=False).head(20).to_string())
else:
    print("No 'strategy' column found in attribution.")

if "date" in df.columns:
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    print("Date range:", df['date'].min(), "->", df['date'].max())
else:
    print("No 'date' column found.")

if "total_return" in df.columns:
    print("Null total_return count:", df['total_return'].isna().sum())
    mask = df['strategy'].astype(str).str.upper().str.contains('LONG_SHORT', na=False)
    print("Sample LONG_SHORT rows (up to 10):")
    print(df[mask].head(10).to_string(index=False))
else:
    print("No 'total_return' column found.")
