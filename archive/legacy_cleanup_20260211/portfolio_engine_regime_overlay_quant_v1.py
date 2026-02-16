r"""
Quant v1.0 — portfolio_engine_regime_overlay_quant_v1.py
Version: v1.0 (full regeneration)

Purpose
- Apply regime-based controls to the raw portfolio engine output.
- Controls:
    - risk_multiplier → scales target weights
    - max_turnover → caps daily turnover

Inputs
- C:\Quant\data\portfolio\quant_portfolio_raw_targets_v1.csv
    Columns:
      - date
      - ticker
      - raw_weight
      - raw_turnover

- C:\Quant\data\analytics\quant_portfolio_controls_v1.csv
    Columns:
      - date
      - target_volatility
      - max_turnover
      - risk_multiplier
      - turnover_cap

Outputs
- C:\Quant\data\portfolio\quant_portfolio_targets_regime_v1.csv
    Columns:
      - date
      - ticker
      - adjusted_weight
      - adjusted_turnover
      - risk_multiplier
      - max_turnover
      - regime_overlay_run_date
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

# ---------------------------------------------------------------------
# Project root and logging
# ---------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("portfolio_engine_regime_overlay_quant_v1")

# ---------------------------------------------------------------------
# File paths
# ---------------------------------------------------------------------
RAW_TARGETS_FILE = PROJECT_ROOT / "data" / "portfolio" / "quant_portfolio_raw_targets_v1.csv"
CONTROLS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_controls_v1.csv"
OUT_FILE = PROJECT_ROOT / "data" / "portfolio" / "quant_portfolio_targets_regime_v1.csv"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------
def load_raw_targets() -> pd.DataFrame:
    logger.info("Loading raw portfolio targets from %s", RAW_TARGETS_FILE)

    df = pd.read_csv(RAW_TARGETS_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "ticker", "raw_weight", "raw_turnover"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Raw targets missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], utc=True)

    return df.sort_values(["date", "ticker"]).reset_index(drop=True)


def load_controls() -> pd.DataFrame:
    logger.info("Loading portfolio controls from %s", CONTROLS_FILE)

    df = pd.read_csv(CONTROLS_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "risk_multiplier", "max_turnover"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Controls file missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], utc=True)

    return df.sort_values("date").reset_index(drop=True)


# ---------------------------------------------------------------------
# Overlay logic
# ---------------------------------------------------------------------
def apply_overlay(raw: pd.DataFrame, controls: pd.DataFrame) -> pd.DataFrame:
    logger.info("Applying regime overlay to portfolio targets.")

    df = raw.merge(controls, on="date", how="left")

    # Enforce numeric types
    df["raw_weight"] = pd.to_numeric(df["raw_weight"], errors="coerce").fillna(0.0)
    df["raw_turnover"] = pd.to_numeric(df["raw_turnover"], errors="coerce").fillna(0.0)
    df["risk_multiplier"] = pd.to_numeric(df["risk_multiplier"], errors="coerce").fillna(1.0)
    df["max_turnover"] = pd.to_numeric(df["max_turnover"], errors="coerce").fillna(0.25)

    # Apply risk multiplier to weights
    df["adjusted_weight"] = df["raw_weight"] * df["risk_multiplier"]

    # Apply turnover cap
    df["adjusted_turnover"] = df["raw_turnover"].clip(upper=df["max_turnover"])

    df = df[
        [
            "date",
            "ticker",
            "adjusted_weight",
            "adjusted_turnover",
            "risk_multiplier",
            "max_turnover",
        ]
    ]

    return df.sort_values(["date", "ticker"]).reset_index(drop=True)


# ---------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------
def save_overlay(df: pd.DataFrame, run_date: str) -> None:
    logger.info("Saving regime-adjusted portfolio targets to %s", OUT_FILE)

    out = df.copy()
    out["regime_overlay_run_date"] = run_date

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False, encoding="utf-8")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    logger.info("Starting portfolio_engine_regime_overlay_quant_v1 run (v1.0).")
    run_date = iso_now()

    raw = load_raw_targets()
    controls = load_controls()
    adjusted = apply_overlay(raw, controls)
    save_overlay(adjusted, run_date)

    logger.info("portfolio_engine_regime_overlay_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()