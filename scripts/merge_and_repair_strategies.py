import pandas as pd
from pathlib import Path

# ---------------------------------------------------------
# INPUT FILES
# ---------------------------------------------------------
LONG_ONLY_FILE = Path(r"C:\Quant\data\analytics\strategy_returns_repaired.parquet")
LS_SOURCE_FILE = Path(r"C:\Quant\data\analytics\strategy_returns.parquet")

# ---------------------------------------------------------
# OUTPUT FILE
# ---------------------------------------------------------
OUTPUT = Path(r"C:\Quant\data\analytics\strategy_returns_merged.parquet")

print("\n[MERGE] Loading LONG_ONLY file:", LONG_ONLY_FILE)
df_lo = pd.read_parquet(LONG_ONLY_FILE)

print("[MERGE] Loading LONG_SHORT source:", LS_SOURCE_FILE)
df_ls = pd.read_parquet(LS_SOURCE_FILE)

# ---------------------------------------------------------
# Keep only LONG_SHORT rows from LS source
# ---------------------------------------------------------
df_ls = df_ls[df_ls["strategy"] == "LONG_SHORT"]

print("\n[MERGE] Strategy counts before merge:")
print("LONG_ONLY:", df_lo["strategy"].value_counts().get("LONG_ONLY", 0))
print("LONG_SHORT:", df_ls["strategy"].value_counts().get("LONG_SHORT", 0))

# ---------------------------------------------------------
# Combine
# ---------------------------------------------------------
df = pd.concat([df_lo, df_ls], ignore_index=True)

# Remove duplicate columns if any
df = df.loc[:, ~df.columns.duplicated()]

# ---------------------------------------------------------
# Normalize weekly_return
# ---------------------------------------------------------
if "weekly_return" in df.columns:
    df["weekly_return"] = pd.to_numeric(df["weekly_return"].astype(str), errors="coerce").fillna(0.0)
elif "total_return" in df.columns:
    df["weekly_return"] = pd.to_numeric(df["total_return"].astype(str), errors="coerce").fillna(0.0)
else:
    df["weekly_return"] = 0.0

# ---------------------------------------------------------
# Recompute cumulative for BOTH strategies
# ---------------------------------------------------------
df = df.sort_values(["strategy", "date"]).reset_index(drop=True)

df["cum_return_recomputed"] = df.groupby("strategy")["weekly_return"].apply(
    lambda s: (1 + s).cumprod()
).reset_index(level=0, drop=True)

df["cum_repaired"] = df["cum_return_recomputed"]

# ---------------------------------------------------------
# Save merged file
# ---------------------------------------------------------
df.to_parquet(OUTPUT, index=False)

print("\n[MERGE] File written:", OUTPUT)

print("\n[MERGE] Final strategy counts:")
print(df["strategy"].value_counts())

print("\n[MERGE] Completed successfully.")