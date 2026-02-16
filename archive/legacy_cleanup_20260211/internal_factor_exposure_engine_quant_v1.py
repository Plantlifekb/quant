r"""
Quant v1.0 — internal_factor_exposure_engine_quant_v1.py
Version: v1.1 (governed upgrade)

Enhancements in v1.1:
- Adds factor_run_date provenance to output.
- Enforces ISO-8601 timezone-aware dates.
- Strengthens schema validation.
- Ensures deterministic, governed behaviour.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("internal_factor_exposure_engine_quant_v1")

FACTORS_SOURCE_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_composite.csv"
OUT_EXPOSURES_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factor_exposures_timeseries.csv"

# Full Option A factor set (intersected with actual columns)
CONFIG_FACTOR_COLS = [
    "mom_short", "mom_medium", "mom_long", "cross_sectional_mom",
    "vol_5", "vol_20", "low_vol_252", "trend_vol_combo",
    "seasonality_12m",
    "mean_reversion",
    "volume", "volume_zscore", "volume_persistence",
    "high_low_spread", "gap", "overnight_ret",
    "component_momentum", "component_trend", "component_seasonality",
    "composite_signal_v1", "model_signal_v2", "model_signal_v3",
]


def load_composite_factors() -> pd.DataFrame:
    logger.info("Loading composite factor dataset from %s", FACTORS_SOURCE_FILE)
    df = pd.read_csv(FACTORS_SOURCE_FILE)

    required = ["date", "ticker"]
    for col in required:
        if col not in df.columns:
            msg = f"Composite factor file must contain '{col}'."
            logger.error(msg)
            raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df = df.dropna(subset=["date", "ticker"])
    df["ticker"] = df["ticker"].astype(str)

    df.columns = [c.lower() for c in df.columns]

    logger.info("Loaded %d rows from composite factor dataset.", len(df))
    return df


def determine_factor_columns(df: pd.DataFrame) -> tuple[list[str], str | None]:
    cols = list(df.columns)
    factor_cols = [c for c in CONFIG_FACTOR_COLS if c in cols]

    missing = [c for c in CONFIG_FACTOR_COLS if c not in cols]
    if missing:
        logger.warning(
            "Missing configured factor columns (skipped): %s",
            ", ".join(missing),
        )

    sector_col = "market_sector" if "market_sector" in cols else None
    if sector_col is None:
        logger.warning("Column 'market_sector' not found. Sector dummies will not be created.")

    if not factor_cols and sector_col is None:
        msg = "No usable factor columns found (neither numeric factors nor sector)."
        logger.error(msg)
        raise ValueError(msg)

    logger.info("Using %d numeric factor columns: %s", len(factor_cols), ", ".join(factor_cols))
    if sector_col:
        logger.info("Using sector column: %s", sector_col)

    return factor_cols, sector_col


def zscore_series(x: pd.Series) -> pd.Series:
    mean = x.mean()
    std = x.std(ddof=1)
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=x.index)
    return (x - mean) / std


def build_exposures(df: pd.DataFrame, factor_cols: list[str], sector_col: str | None) -> pd.DataFrame:
    logger.info("Building per-ticker factor exposures timeseries.")

    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)
    records = []

    for d, g in df.groupby("date"):
        g_local = g.copy()

        out = pd.DataFrame({"date": g_local["date"], "ticker": g_local["ticker"]})

        for col in factor_cols:
            vals = pd.to_numeric(g_local[col], errors="coerce")
            out[col] = zscore_series(vals).astype(float)

        if sector_col is not None and sector_col in g_local.columns:
            sectors = g_local[sector_col].astype(str).fillna("UNKNOWN")
            dummies = pd.get_dummies(sectors, prefix="sector")
            dummies = dummies.reindex(sorted(dummies.columns), axis=1)
            out = pd.concat([out, dummies], axis=1)

        records.append(out)

    exposures = pd.concat(records, axis=0, ignore_index=True)
    exposures = exposures.sort_values(["date", "ticker"]).reset_index(drop=True)
    exposures.columns = [c.lower() for c in exposures.columns]

    logger.info(
        "Built factor exposures matrix with %d rows and %d columns.",
        len(exposures),
        exposures.shape[1],
    )
    return exposures


def save_exposures(df: pd.DataFrame, factor_run_date: str) -> None:
    logger.info("Saving factor exposures timeseries to %s", OUT_EXPOSURES_FILE)
    df = df.copy()
    df["factor_run_date"] = factor_run_date
    df.to_csv(OUT_EXPOSURES_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting internal_factor_exposure_engine_quant_v1 run (v1.1).")

    factor_run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    df = load_composite_factors()
    factor_cols, sector_col = determine_factor_columns(df)
    exposures = build_exposures(df, factor_cols, sector_col)
    save_exposures(exposures, factor_run_date)

    logger.info("internal_factor_exposure_engine_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()