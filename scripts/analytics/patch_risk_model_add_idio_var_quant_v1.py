"""
patch_risk_model_add_idio_var_quant_v1.py

Quant v1.0 — Minimal governed patch to add `idio_var`
to the existing risk_model.parquet so the optimiser
can build a covariance matrix.

Inputs:
    C:\Quant\data\ingestion\risk_model.parquet

Outputs:
    C:\Quant\data\ingestion\risk_model.parquet  (in-place, with backup)

Logic:
    - If `idio_var` already exists, do nothing.
    - Otherwise, add a conservative constant idiosyncratic variance
      corresponding to ~20% idiosyncratic volatility:
          idio_vol = 0.20  ->  idio_var = 0.04
"""

import sys
from pathlib import Path
import shutil
import pandas as pd

BASE = Path(r"C:\Quant")
RISK_MODEL = BASE / "data" / "ingestion" / "risk_model.parquet"
BACKUP = BASE / "data" / "ingestion" / "risk_model_backup_before_idio_var.parquet"


def fail(msg: str):
    print(f"\n❌ FAIL: {msg}\n")
    sys.exit(1)


def ok(msg: str):
    print(f"✔ {msg}")


def main():
    print("\n=== PATCHING RISK MODEL (add idio_var, Quant v1.0) ===\n")

    if not RISK_MODEL.exists():
        fail(f"risk_model.parquet not found: {RISK_MODEL}")

    df = pd.read_parquet(RISK_MODEL)

    if "idio_var" in df.columns:
        ok("risk_model.parquet already has 'idio_var' — no changes made.")
        return

    # Backup original file
    shutil.copy2(RISK_MODEL, BACKUP)
    ok(f"Backup written → {BACKUP}")

    # Add conservative constant idiosyncratic variance
    idio_vol = 0.20  # 20% idiosyncratic volatility
    idio_var = idio_vol ** 2  # 0.04

    df["idio_var"] = idio_var

    df.to_parquet(RISK_MODEL, index=False)
    ok(f"Added 'idio_var' column with constant {idio_var} and rewrote {RISK_MODEL}")

    print("\n🎉 Risk model patched successfully (Quant v1.0 minimal viable covariance).\n")


if __name__ == "__main__":
    main()