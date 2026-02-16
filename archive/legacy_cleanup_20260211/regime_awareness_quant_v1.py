r"""
Quant v1.0 — regime_awareness_quant_v1.py
Version: v1.0

1. Module name
- regime_awareness_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Build daily market regime states from the governed factor universe:
  - Uses cross-sectional average return as a market proxy
  - Classifies trend regime (up / down / neutral)
  - Classifies volatility regime (low / high)
  - Produces a numeric regime_score for later use in exposure scaling

4. Inputs
- C:\Quant\data\analytics\quant_factors_composite.csv

  Required columns:
    - date
    - ticker
    - ret

5. Outputs
- C:\Quant\data\analytics\quant_regime_states.csv

  Columns:
    - date
    - market_return
    - market_level
    - trend_lookback_days
    - vol_lookback_days
    - trend_regime
    - vol_regime
    - regime_score

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

logger = get_logger("regime_awareness_quant_v1")

# Files
FACTOR_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_composite.csv"
REGIME_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_regime_states.csv"

# Regime parameters
TREND_LOOKBACK_DAYS = 60
VOL_LOOKBACK_DAYS = 60
TRADING_DAYS_PER_YEAR = 252


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
    return df


def build_market_series(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building cross-sectional market proxy series.")

    # Cross-sectional average return per day as market proxy
    mkt = (
        df.groupby("date", as_index=False)["ret"]
        .mean()
        .rename(columns={"ret": "market_return"})
    )

    mkt = mkt.sort_values("date").reset_index(drop=True)

    # Build a synthetic market level from returns
    mkt["market_level"] = (1.0 + mkt["market_return"]).cumprod()

    return mkt


def classify_regimes(mkt: pd.DataFrame) -> pd.DataFrame:
    logger.info(
        f"Classifying regimes with trend_lookback={TREND_LOOKBACK_DAYS}, "
        f"vol_lookback={VOL_LOOKBACK_DAYS}."
    )

    g = mkt.copy()

    # Trend: rolling cumulative return over TREND_LOOKBACK_DAYS
    # Approximate as rolling sum of returns (small-return assumption)
    g["trend_window_ret"] = (
        g["market_return"]
        .rolling(window=TREND_LOOKBACK_DAYS, min_periods=TREND_LOOKBACK_DAYS)
        .sum()
    )

    # Volatility: rolling std of daily returns, annualized
    rolling_std = (
        g["market_return"]
        .rolling(window=VOL_LOOKBACK_DAYS, min_periods=VOL_LOOKBACK_DAYS)
        .std()
    )
    g["realized_annual_vol"] = rolling_std * np.sqrt(TRADING_DAYS_PER_YEAR)

    # Trend regime
    # Threshold around zero to avoid overreacting to noise
    trend_up_thresh = 0.01   # +1% over lookback
    trend_down_thresh = -0.01  # -1% over lookback

    def _trend_label(x: float) -> str:
        if np.isnan(x):
            return "unknown"
        if x > trend_up_thresh:
            return "up"
        if x < trend_down_thresh:
            return "down"
        return "neutral"

    g["trend_regime"] = g["trend_window_ret"].apply(_trend_label)

    # Vol regime: compare to median realized vol (over available history)
    vol_median = g["realized_annual_vol"].median(skipna=True)

    def _vol_label(x: float) -> str:
        if np.isnan(x):
            return "unknown"
        if x > vol_median:
            return "high"
        return "low"

    g["vol_regime"] = g["realized_annual_vol"].apply(_vol_label)

    # Regime score: simple mapping
    # up & low vol      -> 1.0 (most favourable)
    # up & high vol     -> 0.7
    # neutral & low vol -> 0.6
    # neutral & high vol-> 0.4
    # down & low vol    -> 0.3
    # down & high vol   -> 0.0 (most hostile)
    def _regime_score(trend: str, vol: str) -> float:
        if trend == "up" and vol == "low":
            return 1.0
        if trend == "up" and vol == "high":
            return 0.7
        if trend == "neutral" and vol == "low":
            return 0.6
        if trend == "neutral" and vol == "high":
            return 0.4
        if trend == "down" and vol == "low":
            return 0.3
        if trend == "down" and vol == "high":
            return 0.0
        return 0.5  # fallback for unknown

    g["regime_score"] = g.apply(
        lambda row: _regime_score(row["trend_regime"], row["vol_regime"]),
        axis=1,
    )

    g["trend_lookback_days"] = TREND_LOOKBACK_DAYS
    g["vol_lookback_days"] = VOL_LOOKBACK_DAYS

    out = g[
        [
            "date",
            "market_return",
            "market_level",
            "trend_lookback_days",
            "vol_lookback_days",
            "trend_regime",
            "vol_regime",
            "regime_score",
        ]
    ].copy()

    logger.info("Regime classification completed.")
    return out


def save_output(df: pd.DataFrame) -> None:
    logger.info(f"Saving regime states to {REGIME_FILE}")
    df.to_csv(REGIME_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting regime_awareness_quant_v1 run (v1.0).")

    df = load_factors()
    mkt = build_market_series(df)
    regimes = classify_regimes(mkt)
    save_output(regimes)

    logger.info("regime_awareness_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()