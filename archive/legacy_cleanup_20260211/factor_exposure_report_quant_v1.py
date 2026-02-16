"""
Quant v1.1 — factor_exposure_report_quant_v1.py
Builds canonical factor exposure timeseries from the risk ensemble file.

Input:
- quant_factors_ensemble_risk_v1.csv
  (date, ticker, ret, size_factor, vol_factor, liquidity_factor, sector_*)

Outputs:
- quant_factor_exposures_timeseries.csv
  (date, ticker, size_factor, vol_factor, liquidity_factor, sector_*, ...)
- quant_factor_exposures_summary.csv
  (factor_name, avg_abs_exposure, exposure_run_date)

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

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("factor_exposure_report_quant_v1")

RISK_ENSEMBLE_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_ensemble_risk_v1.csv"
EXPOSURES_OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factor_exposures_timeseries.csv"
SUMMARY_OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factor_exposures_summary.csv"


def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def main():
    logger.info("Starting factor_exposure_report_quant_v1 (v1.1).")

    # -----------------------------------------------------
    # Load risk ensemble (exposures + returns)
    # -----------------------------------------------------
    df = pd.read_csv(RISK_ENSEMBLE_FILE)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], utc=True)

    # Identify non-factor columns
    non_factor_cols = {
        "date", "ticker", "ret", "composite_signal_v1", "composite_mh_signal",
        "regime_score", "market_sector", "composite_v1_z", "composite_mh_z",
        "ensemble_signal_v1", "ensemble_signal_v1_resid"
    }

    factor_cols = [c for c in df.columns if c not in non_factor_cols]

    if len(factor_cols) == 0:
        raise ValueError("No factor columns found in quant_factors_ensemble_risk_v1.csv")

    # -----------------------------------------------------
    # Build exposures timeseries (wide panel)
    # -----------------------------------------------------
    exposures = df[["date", "ticker"] + factor_cols].copy()
    exposures = exposures.sort_values(["date", "ticker"]).reset_index(drop=True)
    exposures["exposure_run_date"] = iso_now()

    EXPOSURES_OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    exposures.to_csv(EXPOSURES_OUT_FILE, index=False, encoding="utf-8")

    # -----------------------------------------------------
    # Build simple summary (avg |exposure| per factor)
    # -----------------------------------------------------
    summary_rows = []
    for f in factor_cols:
        avg_abs = float(np.nanmean(np.abs(exposures[f].values)))
        summary_rows.append(
            {
                "factor_name": f,
                "avg_abs_exposure": avg_abs,
                "exposure_run_date": iso_now(),
            }
        )

    summary = pd.DataFrame(summary_rows)
    summary = summary.sort_values("factor_name").reset_index(drop=True)
    summary.to_csv(SUMMARY_OUT_FILE, index=False, encoding="utf-8")

    logger.info("factor_exposure_report_quant_v1 (v1.1) completed successfully.")


if __name__ == "__main__":
    main()