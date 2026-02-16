import pandas as pd
from pathlib import Path

BASE = Path(r"C:\Quant\data\analytics")

fixed_path     = BASE / "strategy_returns_fixed.parquet"
bad_path       = BASE / "strategy_returns.parquet"
backup_path    = BASE / "strategy_returns_backup_before_repair.parquet"
repaired_path  = BASE / "strategy_returns_repaired.parquet"

print("Loading files...")
fixed = pd.read_parquet(fixed_path)
bad   = pd.read_parquet(bad_path)

# -----------------------------
# SAFETY CHECKS
# -----------------------------
required_cols = ["date", "strategy", "total_return"]
for col in required_cols:
    if col not in bad.columns:
        raise ValueError(f"'bad' file missing column: {col}")
    if col not in fixed.columns:
        raise ValueError(f"'fixed' file missing column: {col}")

# Align and confirm total_return is identical
merged = fixed.merge(
    bad,
    on=["date", "strategy"],
    how="inner",
    suffixes=("_fixed", "_bad"),
)

if len(merged) != len(fixed):
    raise ValueError("Row count mismatch between fixed and bad after merge.")

diff = (merged["total_return_fixed"] - merged["total_return_bad"]).abs().max()
print(f"Max abs diff in total_return: {diff:.12f}")
if diff > 1e-12:
    raise ValueError("total_return differs between fixed and bad; aborting repair.")

# -----------------------------
# BACKUP ORIGINAL FILE
# -----------------------------
print("Backing up original bad file...")
if not backup_path.exists():
    bad.to_parquet(backup_path)
    print(f"Backup written to: {backup_path}")
else:
    print(f"Backup already exists at: {backup_path}")

# -----------------------------
# REPAIR CUMULATIVE RETURNS
# -----------------------------
print("Recomputing cumulative from total_return...")

bad = bad.sort_values(["strategy", "date"])

# Correct, index-aligned cumulative compounding
bad["cum_repaired"] = (
    1 + bad["total_return"]
).groupby(bad["strategy"]).cumprod()

# -----------------------------
# WRITE REPAIRED FILE
# -----------------------------
print("Writing repaired file...")
bad.to_parquet(repaired_path)
print(f"Repaired file written to: {repaired_path}")

print("Done.")