"""
Quant v2.0 — Factor model scaffold

Creates governed placeholder artefacts for the factor model:

    C:\Quant\data\risk\factor_exposures_quant_v2.parquet
    C:\Quant\data\risk\factor_returns_quant_v2.parquet
    C:\Quant\data\risk\factor_covariance_quant_v2.parquet

These are structurally correct, zero-risk placeholders to be
replaced by a real factor model in later steps.
"""

from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(r"C:\Quant")

DATA_ANALYTICS = BASE / "data" / "analytics"
DATA_RISK = BASE / "data" / "risk"

OPT = DATA_ANALYTICS / "optimiser_regime_quant_v1.parquet"

EXPOSURES_OUT = DATA_RISK / "factor_exposures_quant_v2.parquet"
RETURNS_OUT = DATA_RISK / "factor_returns_quant_v2.parquet"
COV_OUT = DATA_RISK / "factor_covariance_quant_v2.parquet"

FACTOR_NAMES = ["F1", "F2", "F3", "F4", "F5"]


def main():
    print("\n=== BUILDING FACTOR MODEL SCAFFOLD (Quant v2.0) ===\n")

    opt = pd.read_parquet(OPT)
    opt["date"] = pd.to_datetime(opt["date"])
    opt["ticker"] = opt["ticker"].astype(str).str.upper()

    dates = sorted(opt["date"].unique())
    tickers = sorted(opt["ticker"].unique())

    # 1) Factor exposures: one row per (date, ticker), K zero factors
    print("• Building factor_exposures_quant_v2.parquet ...")
    exposures_rows = []
    for d in dates:
        for t in tickers:
            row = {"date": d, "ticker": t}
            for f in FACTOR_NAMES:
                row[f] = 0.0
            exposures_rows.append(row)
    exposures = pd.DataFrame(exposures_rows)
    DATA_RISK.mkdir(parents=True, exist_ok=True)
    exposures.to_parquet(EXPOSURES_OUT, index=False)
    print(f"  ✔ Wrote {EXPOSURES_OUT}")

    # 2) Factor returns: one row per date, K zero returns
    print("• Building factor_returns_quant_v2.parquet ...")
    returns = pd.DataFrame(
        [{"date": d, **{f: 0.0 for f in FACTOR_NAMES}} for d in dates]
    )
    returns.to_parquet(RETURNS_OUT, index=False)
    print(f"  ✔ Wrote {RETURNS_OUT}")

    # 3) Factor covariance: one row per date, flattened KxK identity
    print("• Building factor_covariance_quant_v2.parquet ...")
    cov_rows = []
    k = len(FACTOR_NAMES)
    base_cov = np.eye(k, dtype=float)
    for d in dates:
        row = {"date": d}
        for i, fi in enumerate(FACTOR_NAMES):
            for j, fj in enumerate(FACTOR_NAMES):
                row[f"cov_{fi}_{fj}"] = float(base_cov[i, j])
        cov_rows.append(row)
    cov = pd.DataFrame(cov_rows)
    cov.to_parquet(COV_OUT, index=False)
    print(f"  ✔ Wrote {COV_OUT}")

    print("\n🎉 Factor model scaffold built successfully (Quant v2.0 placeholder).\n")


if __name__ == "__main__":
    main()