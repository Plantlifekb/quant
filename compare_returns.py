import pandas as pd
from pathlib import Path

fixed = pd.read_parquet(r"C:\Quant\data\analytics\strategy_returns_fixed.parquet")
bad   = pd.read_parquet(r"C:\Quant\data\analytics\strategy_returns.parquet")

# Align on date
merged = fixed.merge(
    bad,
    on="date",
    how="inner",
    suffixes=("_fixed", "_bad")
)

# Compute differences
merged["diff"] = merged["total_return_fixed"] - merged["total_return_bad"]

print("Summary of differences:")
print(merged["diff"].describe())

print("\nLargest differences:")
print(merged.sort_values("diff", ascending=False).head(10))

print("\nSmallest differences:")
print(merged.sort_values("diff", ascending=True).head(10))