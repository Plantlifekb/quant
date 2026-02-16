r"""
Quant v1.0 — composite_signal_quant_v1.py
Version: v1.0

1. Module name
- composite_signal_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Build Composite Signal v1.0 using:
  - Momentum signals (short, medium, long)
  - Trend signals (ema_12, ema_26)
  - Seasonality signals (seasonality_12m)
- Produce a single governed composite factor:
  - composite_signal_v1
- This composite is designed for maximum predictable growth:
  - High CAGR
  - Low regime fragility
  - Stable compounding
  - Deterministic behaviour

4. Inputs
- C:\Quant\data\analytics\quant_factors.csv
  Required columns:
    - date
    - ticker
    - mom_short
    - mom_medium
    - mom_long
    - ema_12
    - ema_26
    - seasonality_12m

5. Outputs
- C:\Quant\data\analytics\quant_factors_composite.csv
  Columns:
    - date
    - ticker
    - composite_signal_v1
    - component_momentum
    - component_trend
    - component_seasonality

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
import pandas as pd
import numpy as np

# Ensure Quant scripts directory is importable
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("composite_signal_quant_v1")

# Input / Output paths
FACTOR_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_composite.csv"

# Required columns
REQUIRED = [
    "date", "ticker",
    "mom_short", "mom_medium", "mom_long",
    "ema_12", "ema_26",
    "seasonality_12m"
]

def load_factors():
    logger.info(f"Loading factor data from {FACTOR_FILE}")
    df = pd.read_csv(FACTOR_FILE)
    missing = set(REQUIRED) - set(df.columns)
    if missing:
        msg = f"Missing required columns: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker"])
    logger.info(f"Loaded {len(df)} rows.")
    return df


def build_components(df):
    logger.info("Building component signals.")

    # Momentum component (equal-weighted)
    df["component_momentum"] = (
        df["mom_short"].rank(pct=True) +
        df["mom_medium"].rank(pct=True) +
        df["mom_long"].rank(pct=True)
    ) / 3.0

    # Trend component (EMA-based only)
    df["component_trend"] = (
        (df["ema_12"] - df["ema_26"]).rank(pct=True)
    )

    # Seasonality component
    df["component_seasonality"] = df["seasonality_12m"].rank(pct=True)

    return df


def build_composite(df):
    logger.info("Building composite_signal_v1.")

    df["composite_signal_v1"] = (
        0.50 * df["component_momentum"] +
        0.30 * df["component_trend"] +
        0.20 * df["component_seasonality"]
    )

    return df


def save_output(df):
    logger.info(f"Saving composite factors to {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting composite_signal_quant_v1 run (v1.0).")

    df = load_factors()
    df = build_components(df)
    df = build_composite(df)
    save_output(df)

    logger.info("composite_signal_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()