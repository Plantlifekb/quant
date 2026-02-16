"""
Quant v1.1 — factor_returns_quant_v1.py
Computes canonical (raw) factor returns via cross-sectional regression.

Input:
- quant_factors_ensemble_risk_v1.csv
  (date, ticker, ret, size_factor, vol_factor, liquidity_factor, sector_*)

Output:
- quant_factor_returns_v1.csv
  (date, size_factor, vol_factor, liquidity_factor, sector_*, factor_returns_run_date)

Governance:
- Lowercase columns
- UTC timestamps
- Deterministic behaviour
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import numpy as np
import statsmodels.api as sm

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("factor_returns_quant_v1")

FACTORS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_ensemble_risk_v1.csv"
OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factor_returns_v1.csv"


def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def run_regression(y, X):
    """Run OLS regression and return factor returns (betas)."""
    try:
        X = sm.add_constant(X, has_constant="add")
        model = sm.OLS(y, X).fit()
        return model.params.drop("const")
    except Exception:
        return pd.Series([np.nan] * X.shape[1], index=X.columns)


def main():
    logger.info("Starting factor_returns_quant_v1 (v1.1).")

    # -----------------------------------------------------
    # Load factor exposures + returns
    # -----------------------------------------------------
    df = pd.read_csv(FACTORS_FILE)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], utc=True)

    # Identify factor columns
    non_factor_cols = {
        "date", "ticker", "ret", "composite_signal_v1", "composite_mh_signal",
        "regime_score", "market_sector", "composite_v1_z", "composite_mh_z",
        "ensemble_signal_v1", "ensemble_signal_v1_resid"
    }

    factor_cols = [c for c in df.columns if c not in non_factor_cols]

    if len(factor_cols) == 0:
        raise ValueError("No factor columns found in quant_factors_ensemble_risk_v1.csv")

    # -----------------------------------------------------
    # Compute factor returns per date
    # -----------------------------------------------------
    rows = []

    for date, group in df.groupby("date"):
        y = group["ret"]
        X = group[factor_cols]

        betas = run_regression(y, X)

        row = {"date": date}
        for f in factor_cols:
            row[f] = betas[f]

        rows.append(row)

    out = pd.DataFrame(rows)
    out = out.sort_values("date").reset_index(drop=True)
    out["factor_returns_run_date"] = iso_now()

    # -----------------------------------------------------
    # Save
    # -----------------------------------------------------
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False, encoding="utf-8")

    logger.info("factor_returns_quant_v1 (v1.1) completed successfully.")


if __name__ == "__main__":
    main()