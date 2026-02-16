r"""
Quant v1.0 — risk_model_regime_quant_v1.py
Version: v1.0 (diagonal factor risk, regime-aware)

Purpose
- Build a regime-aware factor risk model from:
    - regime-adjusted factor returns
    - regime states
- Output a diagonal factor risk model:
    - per-factor variance by date, conditioned on regime.

Inputs
- C:\Quant\data\analytics\quant_factor_returns_regime_v1.csv
    Columns:
      - date
      - ticker
      - factor_name
      - factor_return
      - factor_return_regime
      - regime_label
      - factor_weight_multiplier
      - regime_factor_run_date

- C:\Quant\data\analytics\quant_regime_states_v1.csv
    Columns:
      - date
      - regime_label

- C:\Quant\data\analytics\quant_factor_exposures_timeseries.csv
    (Loaded for alignment / future extensions; not strictly required for v1.0.)

Output
- C:\Quant\data\analytics\quant_risk_regime_v1.csv
    Columns:
      - date
      - factor_name
      - factor_var_regime
      - regime_label
      - window_length
      - risk_model_run_date

Governance
- No schema drift.
- All output columns lowercase.
- ISO-8601 dates with UTC.
- Deterministic behaviour.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("risk_model_regime_quant_v1")

FACTOR_RET_REGIME_FILE = (
    PROJECT_ROOT / "data" / "analytics" / "quant_factor_returns_regime_v1.csv"
)
REGIME_STATES_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_regime_states_v1.csv"
FACTOR_EXPOSURES_FILE = (
    PROJECT_ROOT / "data" / "analytics" / "quant_factor_exposures_timeseries.csv"
)
OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_risk_regime_v1.csv"

ROLLING_WINDOW = 60  # days


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------
def load_regime_factor_returns() -> pd.DataFrame:
    logger.info(
        "Loading regime-adjusted factor returns from %s", FACTOR_RET_REGIME_FILE
    )
    df = pd.read_csv(FACTOR_RET_REGIME_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {
        "date",
        "ticker",
        "factor_name",
        "factor_return_regime",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"quant_factor_returns_regime_v1.csv missing columns: {sorted(missing)}"
        )

    df["date"] = pd.to_datetime(df["date"], utc=True)
    df["factor_return_regime"] = pd.to_numeric(
        df["factor_return_regime"], errors="coerce"
    ).fillna(0.0)

    df = df.sort_values(["date", "ticker", "factor_name"]).reset_index(drop=True)
    return df


def load_regime_states() -> pd.DataFrame:
    logger.info("Loading regime states from %s", REGIME_STATES_FILE)
    df = pd.read_csv(REGIME_STATES_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "regime_label"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"quant_regime_states_v1.csv missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values("date").reset_index(drop=True)
    return df


def load_factor_exposures() -> pd.DataFrame:
    """
    Loaded for alignment / future extensions (e.g. idiosyncratic risk).
    Not strictly required for v1.0 diagonal factor risk.
    """
    logger.info(
        "Loading factor exposures timeseries from %s", FACTOR_EXPOSURES_FILE
    )
    df = pd.read_csv(FACTOR_EXPOSURES_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "ticker"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"quant_factor_exposures_timeseries.csv missing id columns: {sorted(missing)}"
        )

    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)
    return df


# ---------------------------------------------------------------------
# Core risk model logic
# ---------------------------------------------------------------------
def compute_factor_var_regime(
    factor_returns_regime: pd.DataFrame,
    regime_states: pd.DataFrame,
    window: int,
) -> pd.DataFrame:
    """
    v1.0 diagonal factor risk:
    - For each date and factor_name, compute cross-sectional mean of regime-adjusted factor returns.
    - Pivot to wide (date × factor).
    - Compute rolling variance per factor over 'window' days.
    - Melt back to long and merge regime labels.
    """
    logger.info(
        "Computing cross-sectional mean regime-adjusted factor returns per date and factor."
    )

    daily_factor_ret = (
        factor_returns_regime.groupby(["date", "factor_name"], as_index=False)[
            "factor_return_regime"
        ]
        .mean()
        .rename(columns={"factor_return_regime": "factor_return_regime_csmean"})
    )

    logger.info("Pivoting to wide format for rolling variance computation.")
    wide = daily_factor_ret.pivot(
        index="date", columns="factor_name", values="factor_return_regime_csmean"
    ).sort_index()

    wide = wide.fillna(0.0)

    logger.info(
        "Computing rolling %d-day variance per factor (diagonal factor risk).", window
    )
    rolling_var = wide.rolling(window=window, min_periods=window).var()

    rolling_var = rolling_var.reset_index().melt(
        id_vars=["date"],
        var_name="factor_name",
        value_name="factor_var_regime",
    )

    rolling_var["factor_var_regime"] = rolling_var["factor_var_regime"].fillna(0.0)

    logger.info("Merging regime labels onto factor variance timeseries.")
    out = rolling_var.merge(regime_states, on="date", how="left")
    out["regime_label"] = out["regime_label"].fillna("unknown")
    out["window_length"] = window

    out = out.sort_values(["date", "factor_name"]).reset_index(drop=True)
    return out


def save_risk_model(df: pd.DataFrame, run_date: str) -> None:
    logger.info("Saving regime-aware risk model to %s", OUT_FILE)
    out = df.copy()
    out["risk_model_run_date"] = run_date

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False, encoding="utf-8")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    logger.info("Starting risk_model_regime_quant_v1 run (v1.0).")
    run_date = iso_now()

    factor_ret_regime = load_regime_factor_returns()
    regime_states = load_regime_states()
    _ = load_factor_exposures()  # reserved for future idiosyncratic risk extensions

    risk_df = compute_factor_var_regime(
        factor_returns_regime=factor_ret_regime,
        regime_states=regime_states,
        window=ROLLING_WINDOW,
    )

    save_risk_model(risk_df, run_date)

    logger.info("risk_model_regime_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()