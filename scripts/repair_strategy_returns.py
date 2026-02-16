import pandas as pd
from pathlib import Path

# -----------------------------------------
# CONFIG
# -----------------------------------------
INPUT = Path(r"C:\Quant\data\analytics\strategy_returns_repaired.parquet")
OUTPUT = Path(r"C:\Quant\data\analytics\strategy_returns_repaired.parquet")  # overwrite in place

print("\n[REPAIR] Loading file:", INPUT)
df = pd.read_parquet(INPUT)

print("[REPAIR] Shape:", df.shape)
print("[REPAIR] Columns:", list(df.columns))

# -----------------------------------------
# VALIDATION: ensure required columns exist
# -----------------------------------------
if "total_return" not in df.columns:
    raise ValueError("Expected column 'total_return' not found.")

# -----------------------------------------
# STEP 1 — Normalize weekly return
# -----------------------------------------
df["weekly_return"] = pd.to_numeric(df["total_return"], errors="coerce").fillna(0.0)

print("\n[REPAIR] weekly_return head:")
print(df[["date", "strategy", "weekly_return"]].head())

# -----------------------------------------
# STEP 2 — Recompute cumulative from scratch
# -----------------------------------------
df = df.sort_values("date").reset_index(drop=True)

df["cum_return_recomputed"] = (1 + df["weekly_return"]).cumprod()

print("\n[REPAIR] cum_return_recomputed tail:")
print(df[["date", "strategy", "cum_return_recomputed"]].tail())

# -----------------------------------------
# STEP 3 — Set cum_repaired = recomputed
# -----------------------------------------
df["cum_repaired"] = df["cum_return_recomputed"]

# -----------------------------------------
# STEP 4 — Preserve original cum column (if exists)
# -----------------------------------------
if "cum" in df.columns:
    print("\n[REPAIR] Original corrupted cum tail:")
    print(df[["date", "strategy", "cum"]].tail())
else:
    print("\n[REPAIR] No original 'cum' column found.")

# -----------------------------------------
# STEP 5 — Write repaired file
# -----------------------------------------
df.to_parquet(OUTPUT, index=False)
print("\n[REPAIR] File written:", OUTPUT)

# -----------------------------------------
# STEP 6 — Final diagnostics
# -----------------------------------------
file_last = df["cum_repaired"].iloc[-1]
recomputed_last = df["cum_return_recomputed"].iloc[-1]
mismatch = 100 * (file_last - recomputed_last) / max(abs(recomputed_last), 1e-9)

print("\n[REPAIR] FINAL CHECK:")
print("file_last:", file_last)
print("recomputed_last:", recomputed_last)
print("mismatch:", mismatch, "%")
print("\n[REPAIR] Completed successfully.")