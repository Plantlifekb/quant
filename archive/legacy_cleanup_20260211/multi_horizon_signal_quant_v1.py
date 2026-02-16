r"""
Quant v1.0 — multi_horizon_signal_quant_v1.py
Version: v1.0

1. Module name
- multi_horizon_signal_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Build a multi-horizon composite signal from governed factor data:
  - Short-horizon signal (e.g. 10-day)
  - Medium-horizon signal (e.g. 30-day)
  - Long-horizon signal (e.g. 90-day)
  - Normalise each horizon cross-sectionally
  - Blend into a single composite_mh signal
  - Preserve narratable, governed behaviour

4. Inputs
- C:\Quant\data\analytics\quant_factors_composite.csv

  Required columns:
    - date
    - ticker
    - ret

5. Outputs
- C:\Quant\data\analytics\quant_factors_composite_mh.csv

  Columns:
    - date
    - ticker
    - ret
    - mh_short_signal
    - mh_medium_signal
    - mh_long_signal
    - composite_mh_signal

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

logger = get_logger("multi_horizon_signal_quant_v1")

# Files
FACTOR_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_composite.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_composite_mh.csv"

# Horizon parameters (in trading days)
SHORT_HORIZON_DAYS = 10
MEDIUM_HORIZON_DAYS = 30
LONG_HORIZON_DAYS = 90


def load_factors() -> pd.DataFrame:
    logger.info(f"Loading factor data from {FACTOR_FILE}")
    df = pd.read_csv(FACTOR_FILE)

    required = {"date", "ticker", "ret"}
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing required columns in factor file: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "ret"])

    df["ret"] = pd.to_numeric(df["ret"], errors="coerce")
    df = df.dropna(subset=["ret"])

    logger.info(f"Loaded {len(df)} rows after cleaning.")
    return df[["date", "ticker", "ret"]]


def _zscore(series: pd.Series) -> pd.Series:
    mean = series.mean()
    std = series.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=series.index)
    return (series - mean) / std


def build_horizon_signals(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(
        "Building multi-horizon signals: "
        f"short={SHORT_HORIZON_DAYS}, medium={MEDIUM_HORIZON_DAYS}, long={LONG_HORIZON_DAYS}."
    )

    # Sort for stable rolling operations
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # For each ticker, compute rolling cumulative returns as horizon signals
    def _per_ticker(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        # Approximate horizon signal as rolling sum of returns (small-return assumption)
        g["mh_short_signal"] = (
            g["ret"]
            .rolling(window=SHORT_HORIZON_DAYS, min_periods=SHORT_HORIZON_DAYS)
            .sum()
        )
        g["mh_medium_signal"] = (
            g["ret"]
            .rolling(window=MEDIUM_HORIZON_DAYS, min_periods=MEDIUM_HORIZON_DAYS)
            .sum()
        )
        g["mh_long_signal"] = (
            g["ret"]
            .rolling(window=LONG_HORIZON_DAYS, min_periods=LONG_HORIZON_DAYS)
            .sum()
        )
        return g

    df = (
        df.groupby("ticker", group_keys=False)
        .apply(_per_ticker)
        .reset_index(drop=True)
    )

    logger.info("Raw horizon signals computed per ticker.")
    return df


def normalise_and_blend(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Normalising horizon signals cross-sectionally and blending into composite_mh_signal.")

    def _per_date(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()

        # Cross-sectional z-scores for each horizon
        for col in ["mh_short_signal", "mh_medium_signal", "mh_long_signal"]:
            g[col] = _zscore(g[col])

        # Simple equal-weight blend of horizons
        g["composite_mh_signal"] = (
            g["mh_short_signal"] +
            g["mh_medium_signal"] +
            g["mh_long_signal"]
        ) / 3.0

        return g

    df = (
        df.groupby("date", group_keys=False)
        .apply(_per_date)
        .reset_index(drop=True)
    )

    logger.info("Multi-horizon signals normalised and blended.")
    return df


def save_output(df: pd.DataFrame) -> None:
    logger.info(f"Saving multi-horizon composite factors to {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting multi_horizon_signal_quant_v1 run (v1.0).")

    df = load_factors()
    df = build_horizon_signals(df)
    df = normalise_and_blend(df)

    out = df[
        [
            "date",
            "ticker",
            "ret",
            "mh_short_signal",
            "mh_medium_signal",
            "mh_long_signal",
            "composite_mh_signal",
        ]
    ].copy()

    save_output(out)

    logger.info("multi_horizon_signal_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()