import pandas as pd
from pathlib import Path

INPUT = Path(r"C:\Quant\data\analytics\strategy_returns_merged.parquet")
OUTPUT = Path(r"C:\Quant\data\analytics\strategy_returns_weekly.parquet")

print("[WEEKLY] Loading merged daily file...")
df = pd.read_parquet(INPUT)

# Ensure date is datetime
df["date"] = pd.to_datetime(df["date"])

# Keep only what we need
cols = [c for c in df.columns if c in ["date", "strategy", "weekly_return"]]
df = df[cols].copy()

# Sort
df = df.sort_values(["strategy", "date"])

print("[WEEKLY] Aggregating daily → weekly (W-FRI)...")
weekly = (
    df.groupby("strategy")
      .resample("W-FRI", on="date")["weekly_return"]
      .apply(lambda s: (1 + s).prod() - 1)
      .reset_index()
)

# Recompute cumulative per strategy
weekly = weekly.sort_values(["strategy", "date"])
weekly["cum_return"] = (
    weekly.groupby("strategy")["weekly_return"]
          .apply(lambda s: (1 + s).cumprod())
          .reset_index(level=0, drop=True)
)

print("[WEEKLY] Weekly file head:")
print(weekly.head())

print("[WEEKLY] Final shape:", weekly.shape)
print("[WEEKLY] Writing:", OUTPUT)
weekly.to_parquet(OUTPUT, index=False)
print("[WEEKLY] Done.")