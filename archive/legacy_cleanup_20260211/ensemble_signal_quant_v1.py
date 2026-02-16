r"""
Quant v1.0 — ensemble_signal_quant_v1.py
Version: v1.1

1. Module name
- ensemble_signal_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Build an ensemble signal by blending:
  - Existing composite signal v1 (composite_signal_v1)
  - Multi-horizon composite signal (composite_mh_signal)
  - Optional regime_score as a stabiliser
- Preserve risk-model inputs (market_sector, low_vol_252, volume_zscore)
- Output a governed ensemble signal for downstream portfolio construction and risk modelling.

4. Inputs
- C:\Quant\data\analytics\quant_factors_composite.csv

  Required columns:
    - date
    - ticker
    - ret
    - composite_signal_v1
    - market_sector
    - low_vol_252
    - volume_zscore

- C:\Quant\data\analytics\quant_factors_composite_mh.csv

  Required columns:
    - date
    - ticker
    - ret
    - composite_mh_signal

- C:\Quant\data\analytics\quant_regime_states.csv

  Required columns:
    - date
    - regime_score

5. Outputs
- C:\Quant\data\analytics\quant_factors_ensemble_v1.csv

  Columns:
    - date
    - ticker
    - ret
    - composite_signal_v1
    - composite_mh_signal
    - regime_score
    - market_sector
    - low_vol_252
    - volume_zscore
    - composite_v1_z
    - composite_mh_z
    - ensemble_signal_v1

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

logger = get_logger("ensemble_signal_quant_v1")

# Files
COMPOSITE_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_composite.csv"
MH_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_composite_mh.csv"
REGIME_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_regime_states.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_ensemble_v1.csv"

# Ensemble weights (governed, explicit)
W_COMPOSITE_V1 = 0.5
W_COMPOSITE_MH = 0.5

# Regime modulation parameters
MIN_REGIME_MULTIPLIER = 0.7
MAX_REGIME_MULTIPLIER = 1.0


def _zscore(series: pd.Series) -> pd.Series:
    mean = series.mean()
    std = series.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=series.index)
    return (series - mean) / std


def load_composite() -> pd.DataFrame:
    logger.info(f"Loading base composite factors from {COMPOSITE_FILE}")
    df = pd.read_csv(COMPOSITE_FILE)

    required = {
        "date",
        "ticker",
        "ret",
        "composite_signal_v1",
        "market_sector",
        "low_vol_252",
        "volume_zscore",
    }
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing required columns in composite file: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(
        subset=[
            "date",
            "ticker",
            "ret",
            "composite_signal_v1",
            "market_sector",
            "low_vol_252",
            "volume_zscore",
        ]
    )

    df["ret"] = pd.to_numeric(df["ret"], errors="coerce")
    df["composite_signal_v1"] = pd.to_numeric(df["composite_signal_v1"], errors="coerce")
    df["low_vol_252"] = pd.to_numeric(df["low_vol_252"], errors="coerce")
    df["volume_zscore"] = pd.to_numeric(df["volume_zscore"], errors="coerce")

    df = df.dropna(subset=["ret", "composite_signal_v1", "low_vol_252", "volume_zscore"])

    logger.info(f"Loaded {len(df)} rows from composite file after cleaning.")
    return df[
        [
            "date",
            "ticker",
            "ret",
            "composite_signal_v1",
            "market_sector",
            "low_vol_252",
            "volume_zscore",
        ]
    ]


def load_multi_horizon() -> pd.DataFrame:
    logger.info(f"Loading multi-horizon composite factors from {MH_FILE}")
    df = pd.read_csv(MH_FILE)

    required = {"date", "ticker", "ret", "composite_mh_signal"}
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing required columns in multi-horizon file: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "ret", "composite_mh_signal"])

    df["ret"] = pd.to_numeric(df["ret"], errors="coerce")
    df["composite_mh_signal"] = pd.to_numeric(df["composite_mh_signal"], errors="coerce")
    df = df.dropna(subset=["ret", "composite_mh_signal"])

    logger.info(f"Loaded {len(df)} rows from multi-horizon file after cleaning.")
    return df[["date", "ticker", "ret", "composite_mh_signal"]]


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


def build_ensemble(
    base: pd.DataFrame,
    mh: pd.DataFrame,
    regimes: pd.DataFrame,
) -> pd.DataFrame:
    logger.info(
        "Building ensemble signal from composite_signal_v1 and composite_mh_signal "
        f"with weights: v1={W_COMPOSITE_V1}, mh={W_COMPOSITE_MH}."
    )

    merged = base.merge(
        mh[["date", "ticker", "composite_mh_signal"]],
        on=["date", "ticker"],
        how="inner",
    )

    merged = merged.merge(regimes, on="date", how="left")
    merged["regime_score"] = merged["regime_score"].fillna(0.5)

    span = MAX_REGIME_MULTIPLIER - MIN_REGIME_MULTIPLIER
    merged["regime_multiplier"] = MIN_REGIME_MULTIPLIER + span * merged["regime_score"]

    def _per_date(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        g["composite_v1_z"] = _zscore(g["composite_signal_v1"])
        g["composite_mh_z"] = _zscore(g["composite_mh_signal"])

        g["ensemble_raw"] = (
            W_COMPOSITE_V1 * g["composite_v1_z"] +
            W_COMPOSITE_MH * g["composite_mh_z"]
        )

        g["ensemble_signal_v1"] = g["ensemble_raw"] * g["regime_multiplier"]
        return g

    out = (
        merged
        .groupby("date", group_keys=False)
        .apply(_per_date)
        .reset_index(drop=True)
    )

    logger.info("Ensemble signal built and regime-modulated.")
    return out[
        [
            "date",
            "ticker",
            "ret",
            "composite_signal_v1",
            "composite_mh_signal",
            "regime_score",
            "market_sector",
            "low_vol_252",
            "volume_zscore",
            "composite_v1_z",
            "composite_mh_z",
            "ensemble_signal_v1",
        ]
    ]


def save_output(df: pd.DataFrame) -> None:
    logger.info(f"Saving ensemble factors to {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting ensemble_signal_quant_v1 run (v1.1).")

    base = load_composite()
    mh = load_multi_horizon()
    regimes = load_regime_states()

    out = build_ensemble(base, mh, regimes)
    save_output(out)

    logger.info("ensemble_signal_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()