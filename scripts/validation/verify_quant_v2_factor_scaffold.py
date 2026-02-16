"""
Quant v2.0 — Updated Factor Model Verifier

Checks that:
    • factor_exposures_quant_v2.parquet contains:
        - sector factors (SEC_*)
        - industry factors (IND_*)
        - style factors (raw + neutral)
    • factor_returns_quant_v2.parquet exists
    • factor_covariance_quant_v2.parquet exists
"""

import sys
from pathlib import Path
import pandas as pd

BASE = Path(r"C:\Quant")
DATA_RISK = BASE / "data" / "risk"

EXPOSURES = DATA_RISK / "factor_exposures_quant_v2.parquet"
RETURNS = DATA_RISK / "factor_returns_quant_v2.parquet"
COV = DATA_RISK / "factor_covariance_quant_v2.parquet"

STYLE_RAW = ["MOM_raw", "VOL_raw", "LIQ_raw", "SIZE_raw", "VALUE_raw"]
STYLE_NEUTRAL = ["MOM_neutral", "VOL_neutral", "LIQ_neutral", "SIZE_neutral", "VALUE_neutral"]


def fail(msg: str):
    print(f"\n❌ FAIL: {msg}\n")
    sys.exit(1)


def ok(msg: str):
    print(f"✔ {msg}")


def main():
    print("\n=== VERIFYING QUANT V2 FACTOR MODEL (UPDATED) ===\n")

    # 1. Existence
    for name, path in [
        ("factor_exposures", EXPOSURES),
        ("factor_returns", RETURNS),
        ("factor_covariance", COV),
    ]:
        if not path.exists():
            fail(f"Missing required risk file: {path}")
        ok(f"Found {name}: {path}")

    # 2. Exposure schema
    exp = pd.read_parquet(EXPOSURES)
    cols = exp.columns.tolist()

    # Required base columns
    for col in ["date", "ticker"]:
        if col not in cols:
            fail(f"factor_exposures missing required column: {col}")
    ok("Base columns present")

    # Sector factors
    sec_cols = [c for c in cols if c.startswith("SEC_")]
    if len(sec_cols) == 0:
        fail("No sector factor columns (SEC_*) found")
    ok(f"Sector factors found: {len(sec_cols)}")

    # Industry factors
    ind_cols = [c for c in cols if c.startswith("IND_")]
    if len(ind_cols) == 0:
        fail("No industry factor columns (IND_*) found")
    ok(f"Industry factors found: {len(ind_cols)}")

    # Style factors (raw)
    for col in STYLE_RAW:
        if col not in cols:
            fail(f"Missing raw style factor: {col}")
    ok("All raw style factors present")

    # Style factors (neutral)
    for col in STYLE_NEUTRAL:
        if col not in cols:
            fail(f"Missing neutral style factor: {col}")
    ok("All sector-neutral style factors present")

    print("\n🎉 VERIFIED: Quant v2 factor model is aligned and governed.\n")


if __name__ == "__main__":
    main()