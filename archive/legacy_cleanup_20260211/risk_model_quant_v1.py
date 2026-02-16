r"""
Quant v1.0 — risk_model_quant_v1.py
Version: v1.0

1. Module name
- risk_model_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Build a lightweight cross-sectional risk model and factor-neutralised ensemble signal:
  - Define simple style/industry risk factors from existing data
  - Estimate cross-sectional betas of ensemble_signal_v1 to those risk factors
  - Compute residual (idiosyncratic) signal: ensemble_signal_v1_resid
  - Output a governed, factor-neutralised signal for portfolio construction

4. Inputs
- C:\Quant\data\analytics\quant_factors_ensemble_v1.csv

  Required columns:
    - date
    - ticker
    - ret
    - ensemble_signal_v1
    - market_sector
    - low_vol_252
    - volume_zscore
    - size_proxy (optional; if absent, we derive from volume)

5. Outputs
- C:\Quant\data\analytics\quant_factors_ensemble_risk_v1.csv

  Columns:
    - date
    - ticker
    - ret
    - ensemble_signal_v1
    - market_sector
    - low_vol_252
    - volume_zscore
    - size_factor
    - vol_factor
    - liquidity_factor
    - sector_* dummies
    - ensemble_signal_v1_resid

6. Governance rules
- No schema drift.
- No silent changes.
- All output columns lowercase.
- ISO-8601 dates only.
- Deterministic, reproducible behaviour.
- No writing outside governed directories.

7. Logging rules
- Uses logging_quant_v1.py
- Logs start, end, and key events.
- Logs errors narratably.

8. Encoding rules
- All CSV outputs UTF-8.

9. Dependencies
- pandas
- numpy
- logging_quant_v1

10. Provenance
- This module is a governed component of Quant v1.0.
- Any modification requires version bump and architecture update.
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd

# Project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("risk_model_quant_v1")

# Files
ENSEMBLE_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_ensemble_v1.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_ensemble_risk_v1.csv"


def _zscore(series: pd.Series) -> pd.Series:
    mean = series.mean()
    std = series.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=series.index)
    return (series - mean) / std


def load_ensemble() -> pd.DataFrame:
    logger.info(f"Loading ensemble factors from {ENSEMBLE_FILE}")
    df = pd.read_csv(ENSEMBLE_FILE)

    required = {"date", "ticker", "ret", "ensemble_signal_v1", "market_sector", "low_vol_252", "volume_zscore"}
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing required columns in ensemble file for risk model: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "ret", "ensemble_signal_v1", "market_sector", "low_vol_252", "volume_zscore"])

    df["ret"] = pd.to_numeric(df["ret"], errors="coerce")
    df["ensemble_signal_v1"] = pd.to_numeric(df["ensemble_signal_v1"], errors="coerce")
    df["low_vol_252"] = pd.to_numeric(df["low_vol_252"], errors="coerce")
    df["volume_zscore"] = pd.to_numeric(df["volume_zscore"], errors="coerce")

    df = df.dropna(subset=["ret", "ensemble_signal_v1", "low_vol_252", "volume_zscore"])

    logger.info(f"Loaded {len(df)} rows for risk model after cleaning.")
    return df


def build_style_factors(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building style factors (size, volatility, liquidity).")

    g = df.copy()

    # Size proxy: if explicit size column exists, use it; otherwise derive from volume_zscore
    if "size_proxy" in g.columns:
        g["size_factor"] = _zscore(pd.to_numeric(g["size_proxy"], errors="coerce").fillna(0.0))
    else:
        # Invert volume_zscore as a crude size proxy (higher volume ~ larger)
        g["size_factor"] = _zscore(g["volume_zscore"])

    # Volatility factor: low_vol_252 (lower vol = more defensive)
    g["vol_factor"] = _zscore(g["low_vol_252"])

    # Liquidity factor: volume_zscore directly
    g["liquidity_factor"] = _zscore(g["volume_zscore"])

    return g


def build_sector_dummies(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building sector dummy factors.")

    g = df.copy()
    # Ensure sector is string
    g["market_sector"] = g["market_sector"].astype(str).fillna("unknown")

    sector_dummies = pd.get_dummies(g["market_sector"], prefix="sector", dtype=float)
    g = pd.concat([g, sector_dummies], axis=1)

    return g


def compute_residual_signal(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing factor-neutralised ensemble_signal_v1_resid via cross-sectional regression per date.")

    g = df.copy()

    # Identify factor columns
    factor_cols = [c for c in g.columns if c.startswith("sector_")]
    factor_cols += ["size_factor", "vol_factor", "liquidity_factor"]

    def _per_date(group: pd.DataFrame) -> pd.DataFrame:
        d = group.copy()

        y = d["ensemble_signal_v1"].values.astype(float)

        X = d[factor_cols].values.astype(float)
        # Add intercept
        X = np.column_stack([np.ones(len(d)), X])

        # If too few names, skip regression
        if X.shape[0] <= X.shape[1]:
            d["ensemble_signal_v1_resid"] = y - y.mean()
            return d

        # OLS: beta = (X'X)^(-1) X'y
        try:
            XtX = X.T @ X
            XtX_inv = np.linalg.pinv(XtX)
            beta = XtX_inv @ (X.T @ y)
            y_hat = X @ beta
            resid = y - y_hat
        except Exception as e:
            logger.warning(f"Regression failed on date {d['date'].iloc[0]}: {e}. Using demeaned signal.")
            resid = y - y.mean()

        d["ensemble_signal_v1_resid"] = resid
        return d

    out = (
        g.groupby("date", group_keys=False)
        .apply(_per_date)
        .reset_index(drop=True)
    )

    logger.info("Factor-neutralised residual signal computed.")
    return out


def save_output(df: pd.DataFrame) -> None:
    logger.info(f"Saving risk-model-enhanced ensemble factors to {OUTPUT_FILE}")

    cols = [c for c in df.columns if c not in ["size_proxy"]]  # drop any raw helper
    df[cols].to_csv(OUTPUT_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting risk_model_quant_v1 run (v1.0).")

    df = load_ensemble()
    df = build_style_factors(df)
    df = build_sector_dummies(df)
    df = compute_residual_signal(df)
    save_output(df)

    logger.info("risk_model_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()