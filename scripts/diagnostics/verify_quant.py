# C:\Quant\scripts\diagnostics\verify_quant.py
import pandas as pd, json, sys
from pathlib import Path
MAN = Path("C:/Quant/data/analytics/strategy_returns_manifest.json")
OUT = Path("C:/Quant/data/analytics/strategy_returns.parquet")
if not OUT.exists() or not MAN.exists():
    print("Missing outputs; run builder first")
    sys.exit(2)
m = json.loads(MAN.read_text())
df = pd.read_parquet(OUT)
problems = []
if not pd.to_numeric(df['cum'], errors='coerce').notna().all():
    problems.append("Non-finite cum values")
dates = df['date'].dropna().sort_values().unique()
if len(dates) == 0:
    problems.append("No dates")
if set(df['strategy'].unique()) != {'LONG_ONLY','LONG_SHORT'}:
    problems.append("Missing strategy labels")
finals = df.groupby('strategy')['cum'].last().to_dict()
print("MANIFEST:", m)
print("FINAL CUMS:", finals)
if problems:
    print("PROBLEMS:", problems)
    sys.exit(3)
print("VERIFICATION OK")
sys.exit(0)