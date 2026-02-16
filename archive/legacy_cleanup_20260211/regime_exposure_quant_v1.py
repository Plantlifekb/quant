r"""
Quant v1.0 — regime_exposure_quant_v1.py
Version: v1.0

1. Module name
- regime_exposure_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Apply regime-adaptive exposure scaling to an already volatility-targeted portfolio:
  - Input:
      - quant_portfolio_performance_voltarget.csv
      - quant_regime_states.csv
  - Output:
      - quant_portfolio_performance_regime_vt.csv
- For each date and portfolio_type:
  - Read vt_daily_return (vol-targeted daily return)
  - Read regime_score (0.0 → 1.0)
  - Compute a regime_scaling_factor
  - Generate regime- and vol-targeted daily and cumulative returns

4. Inputs
- C:\Quant\data\analytics\quant_portfolio_performance_voltarget.csv

  Required columns:
    - date
    - portfolio_type
    - daily_return
    - cumulative_return
    - vt_scaling_factor
    - vt_daily_return
    - vt_cumulative_return

- C:\Quant\data\analytics\quant_regime_states.csv

  Required columns:
    - date
    - regime_score

5. Outputs
- C:\Quant\data\analytics\quant_portfolio_performance_regime_vt.csv

  Columns:
    - date
    - portfolio_type
    - daily_return
    - cumulative_return
    - vt_scaling_factor
    - vt_daily_return
    - vt_cumulative_return
    - regime_score
    - regime_scaling_factor
    - ra_vt_daily_return
    - ra_vt_cumulative_return

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

logger = get_logger("regime_exposure_quant_v1")

# Files
PERF_VT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance_voltarget.csv"
REGIME_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_regime_states.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance_regime_vt.csv"

# Regime exposure parameters
# regime_score is in [0, 1]; we map it to [MIN_REGIME_SCALING, MAX_REGIME_SCALING]
MIN_REGIME_SCALING = 0.2
MAX_REGIME_SCALING = 1.0


def load_voltarget_performance() -> pd.DataFrame:
    logger.info(f"Loading volatility-targeted performance from {PERF_VT_FILE}")
    df = pd.read_csv(PERF_VT_FILE)

    required = {
        "date",
        "portfolio_type",
        "daily_return",
        "cumulative_return",
        "vt_scaling_factor",
        "vt_daily_return",
        "vt_cumulative_return",
    }
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing required columns in vol-target performance file: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "portfolio_type", "vt_daily_return"])

    df["vt_daily_return"] = pd.to_numeric(df["vt_daily_return"], errors="coerce")
    df = df.dropna(subset=["vt_daily_return"])

    logger.info(f"Loaded {len(df)} rows after cleaning vol-target performance.")
    return df


def load_regime_states() -> pd.DataFrame:
    logger.info(f"Loading regime states from {REGIME_FILE}")
    reg = pd.read_csv(REGIME_FILE)

    required = {"date", "regime_score"}
    missing = required - set(reg.columns)
    if missing:
        msg = f"Missing required columns in regime states file: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    reg["date"] = pd.to_datetime(reg["date"], errors="coerce")
    reg = reg.dropna(subset=["date", "regime_score"])

    reg["regime_score"] = pd.to_numeric(reg["regime_score"], errors="coerce")
    reg = reg.dropna(subset=["regime_score"])

    logger.info(f"Loaded {len(reg)} regime rows after cleaning.")
    return reg[["date", "regime_score"]]


def apply_regime_exposure(perf_vt: pd.DataFrame, regimes: pd.DataFrame) -> pd.DataFrame:
    logger.info("Applying regime-adaptive exposure scaling.")

    # Merge on date
    merged = perf_vt.merge(regimes, on="date", how="left")

    # For dates without regime_score (early history), default to neutral scaling (1.0)
    merged["regime_score"] = merged["regime_score"].fillna(0.5)

    # Map regime_score in [0,1] to [MIN_REGIME_SCALING, MAX_REGIME_SCALING]
    span = MAX_REGIME_SCALING - MIN_REGIME_SCALING
    merged["regime_scaling_factor"] = MIN_REGIME_SCALING + span * merged["regime_score"]

    # Apply regime scaling on top of vol-targeted returns
    merged["ra_vt_daily_return"] = merged["vt_daily_return"] * merged["regime_scaling_factor"]

    # Regime- and vol-targeted cumulative return as running sum per portfolio_type
    merged = merged.sort_values(["portfolio_type", "date"]).reset_index(drop=True)
    merged["ra_vt_cumulative_return"] = (
        merged.groupby("portfolio_type")["ra_vt_daily_return"].cumsum()
    )

    logger.info("Regime-adaptive exposure scaling applied.")
    return merged[
        [
            "date",
            "portfolio_type",
            "daily_return",
            "cumulative_return",
            "vt_scaling_factor",
            "vt_daily_return",
            "vt_cumulative_return",
            "regime_score",
            "regime_scaling_factor",
            "ra_vt_daily_return",
            "ra_vt_cumulative_return",
        ]
    ]


def save_output(df: pd.DataFrame) -> None:
    logger.info(f"Saving regime- and vol-targeted performance to {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting regime_exposure_quant_v1 run (v1.0).")

    perf_vt = load_voltarget_performance()
    regimes = load_regime_states()
    out = apply_regime_exposure(perf_vt, regimes)
    save_output(out)

    logger.info("regime_exposure_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()