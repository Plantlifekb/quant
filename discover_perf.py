import pandas as pd
from pathlib import Path

candidates = [
    Path(r"C:\Quant\data\analytics\strategy_returns_fixed.parquet"),
    Path(r"C:\Quant\data\analytics\strategy_returns.parquet"),
    Path(r"C:\Quant\data\analytics\portfolio_performance_quant_v1.parquet"),
    Path(r"C:\Quant\data\analytics\performance_quant_v2.parquet"),
    Path(r"C:\Quant\data\weekly_portfolio.parquet"),
]

def summarize(path):
    if not path.exists():
        print(f"[MISS] {path} (does not exist)")
        return

    df = pd.read_parquet(path)
    cols = set(df.columns)

    if "date" not in cols:
        print(f"[SKIP] {path} (no date column)")
        return

    for cand in ["total_return", "weekly_return", "strategy_return", "return"]:
        if cand in cols:
            r = df[cand].fillna(0)
            ann = (1 + r.mean())**52 - 1
            print(f"[HIT] {path}")
            print("  columns:", sorted(cols))
            print(f"  weekly mean (last 4): {r.tail(4).mean():.6f}")
            print(f"  annualized: {ann:.6f}")
            return

    print(f"[SKIP] {path} (no obvious weekly return column)")

for p in candidates:
    summarize(p)