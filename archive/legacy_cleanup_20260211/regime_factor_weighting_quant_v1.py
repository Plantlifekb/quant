"""
Quant v1.3 — regime_factor_weighting_quant_v1.py
Regime-aware factor weighting engine.

Enhancements vs v1.2:
- Cleaner regime × trend logic
- Explicit factor groups
- Deterministic ordering
- Full governance (schema, UTC, lowercase)
- Alignment with optimiser_regime_quant_v1.1
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("regime_factor_weighting_quant_v1")

FACTORS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors.csv"
REGIME_STATES_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_regime_states_v1.csv"
REGIME_ADJ_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_regime_adjustments_v1.csv"
OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factor_returns_regime_v1.csv"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------
# FACTOR GROUPS (governed)
# ---------------------------------------------------------

MOMENTUM = {
    "mom_short",
    "mom_medium",
    "mom_long",
    "cross_sectional_mom",
    "model_signal_v2",
    "model_signal_v3",
}

MEAN_REVERSION = {"mean_reversion"}

LOW_VOL = {"low_vol_252"}

CARRY_PERSISTENCE = {
    "volume_persistence",
    "seasonality_12m",
    "trend_vol_combo",
}


# ---------------------------------------------------------
# LOADERS
# ---------------------------------------------------------

def load_factors() -> pd.DataFrame:
    logger.info("Loading factor returns from %s", FACTORS_FILE)
    df = pd.read_csv(FACTORS_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "ticker"}
    if missing := required - set(df.columns):
        raise ValueError(f"quant_factors.csv missing id columns: {sorted(missing)}")

    factor_cols = (
        MOMENTUM
        | MEAN_REVERSION
        | LOW_VOL
        | CARRY_PERSISTENCE
    )

    existing = [c for c in factor_cols if c in df.columns]
    if not existing:
        raise ValueError("No recognised factor columns found in quant_factors.csv")

    df["date"] = pd.to_datetime(df["date"], utc=True)

    long_df = df.melt(
        id_vars=["date", "ticker"],
        value_vars=existing,
        var_name="factor_name",
        value_name="factor_return",
    )

    long_df["factor_return"] = pd.to_numeric(long_df["factor_return"], errors="coerce").fillna(0.0)

    return long_df.sort_values(["date", "ticker", "factor_name"]).reset_index(drop=True)


def load_regime_states() -> pd.DataFrame:
    df = pd.read_csv(REGIME_STATES_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "regime_label"}
    if missing := required - set(df.columns):
        raise ValueError(f"quant_regime_states_v1.csv missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    return df.sort_values("date").reset_index(drop=True)


def load_regime_adjustments() -> pd.DataFrame:
    df = pd.read_csv(REGIME_ADJ_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "regime_label"}
    if missing := required - set(df.columns):
        raise ValueError(f"quant_regime_adjustments_v1.csv missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    return df.sort_values("date").reset_index(drop=True)


# ---------------------------------------------------------
# REGIME × TREND MULTIPLIERS
# ---------------------------------------------------------

def base_multiplier(regime_core: str, factor: str) -> float:
    regime_core = regime_core.lower()
    factor = factor.lower()

    if regime_core in {"calm", "normal"}:
        if factor in MOMENTUM: return 1.10
        if factor in MEAN_REVERSION: return 1.00
        if factor in LOW_VOL: return 0.95
        if factor in CARRY_PERSISTENCE: return 1.05
        return 1.00

    if regime_core == "volatile":
        if factor in MOMENTUM: return 0.80
        if factor in MEAN_REVERSION: return 1.10
        if factor in LOW_VOL: return 1.20
        if factor in CARRY_PERSISTENCE: return 1.00
        return 1.00

    if regime_core in {"crisis", "stress"}:
        if factor in MOMENTUM: return 0.50
        if factor in MEAN_REVERSION: return 1.00
        if factor in LOW_VOL: return 1.30
        if factor in CARRY_PERSISTENCE: return 0.90
        return 1.00

    return 1.00


def trend_multiplier(trend: str, factor: str) -> float:
    trend = trend.lower()
    factor = factor.lower()

    if trend == "trending_up":
        if factor in MOMENTUM: return 1.10
        if factor in CARRY_PERSISTENCE: return 1.10
        if factor in MEAN_REVERSION: return 0.90
        if factor in LOW_VOL: return 0.95
        return 1.00

    if trend == "trending_down":
        if factor in MOMENTUM: return 0.90
        if factor in CARRY_PERSISTENCE: return 0.90
        if factor in MEAN_REVERSION: return 1.10
        if factor in LOW_VOL: return 1.05
        return 1.00

    return 1.00


def get_factor_multiplier(regime_label: str, factor_name: str) -> float:
    regime_label = regime_label.lower()

    if "_" in regime_label:
        core, trend = regime_label.split("_", 1)
    else:
        core, trend = regime_label, ""

    return base_multiplier(core, factor_name) * trend_multiplier(trend, factor_name)


# ---------------------------------------------------------
# CORE ENGINE
# ---------------------------------------------------------

def apply_regime_factor_weighting(factors: pd.DataFrame, regime_states: pd.DataFrame) -> pd.DataFrame:
    logger.info("Applying regime × trend factor multipliers")

    df = factors.merge(regime_states, on="date", how="left")
    df["regime_label"] = df["regime_label"].fillna("unknown")

    df["factor_weight_multiplier"] = df.apply(
        lambda r: get_factor_multiplier(r["regime_label"], r["factor_name"]),
        axis=1,
    )

    df["factor_return_regime"] = df["factor_return"] * df["factor_weight_multiplier"]

    return df[
        [
            "date",
            "ticker",
            "factor_name",
            "factor_return",
            "factor_return_regime",
            "regime_label",
            "factor_weight_multiplier",
        ]
    ].sort_values(["date", "ticker", "factor_name"]).reset_index(drop=True)


def save_regime_factor_returns(df: pd.DataFrame, run_date: str) -> None:
    df = df.copy()
    df["regime_factor_run_date"] = run_date

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_FILE, index=False, encoding="utf-8")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main() -> None:
    logger.info("Starting regime_factor_weighting_quant_v1 (v1.3)")
    run_date = iso_now()

    factors = load_factors()
    regime_states = load_regime_states()
    _ = load_regime_adjustments()  # reserved for future use

    out = apply_regime_factor_weighting(factors, regime_states)
    save_regime_factor_returns(out, run_date)

    logger.info("Completed regime_factor_weighting_quant_v1 (v1.3)")


if __name__ == "__main__":
    main()