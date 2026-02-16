r"""
Quant v1.0 — regime_adjustments_quant_v1.py
Version: v1.0

Purpose
- Map regime_label into numeric controls:
  - risk_multiplier (e.g. target volatility / leverage)
  - turnover_cap (max daily turnover fraction)
- Provide a governed, externalised regime control surface.

Inputs
- C:\Quant\data\analytics\quant_regime_states_v1.csv
    Columns:
      - date
      - realised_vol_20d
      - realised_vol_60d
      - drawdown_60d
      - regime_label

Outputs
- C:\Quant\data\analytics\quant_regime_adjustments_v1.csv
    Columns:
      - date
      - regime_label
      - risk_multiplier
      - turnover_cap
      - regime_adjustments_run_date
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("regime_adjustments_quant_v1")

REGIME_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_regime_states_v1.csv"
OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_regime_adjustments_v1.csv"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def load_regimes() -> pd.DataFrame:
    logger.info("Loading regime states from %s", REGIME_FILE)
    df = pd.read_csv(REGIME_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "regime_label"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Regime states missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values("date").reset_index(drop=True)
    logger.info("Loaded %d regime observations.", len(df))
    return df


def map_regime_to_controls(regime: str) -> tuple[float, float]:
    regime = (regime or "").lower()

    # Risk multiplier: 1.0 = baseline
    # Turnover cap: fraction of notional per day
    if regime == "crisis":
        return 0.4, 0.10   # very low risk, very tight turnover
    if regime == "high_vol":
        return 0.6, 0.15
    if regime == "stress_building":
        return 0.7, 0.20
    if regime == "calm_trending_up":
        return 1.2, 0.30
    if regime == "calm_trending_down":
        return 0.8, 0.20
    if regime == "recovery":
        return 1.0, 0.25
    if regime == "normal":
        return 1.0, 0.25

    # unknown / fallback
    return 0.8, 0.20


def build_adjustments(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building regime-based risk and turnover controls.")
    out = df[["date", "regime_label"]].copy()

    controls = out["regime_label"].apply(map_regime_to_controls)
    out["risk_multiplier"] = controls.apply(lambda x: x[0])
    out["turnover_cap"] = controls.apply(lambda x: x[1])

    out = out.sort_values("date").reset_index(drop=True)
    out.columns = [c.lower() for c in out.columns]
    logger.info("Built adjustments for %d dates.", len(out))
    return out


def save_adjustments(df: pd.DataFrame, run_date: str) -> None:
    logger.info("Saving regime adjustments to %s", OUT_FILE)
    out = df.copy()
    out["regime_adjustments_run_date"] = run_date
    out.to_csv(OUT_FILE, index=False, encoding="utf-8")


def main() -> None:
    logger.info("Starting regime_adjustments_quant_v1 run (v1.0).")
    run_date = iso_now()

    regimes = load_regimes()
    adjustments = build_adjustments(regimes)
    save_adjustments(adjustments, run_date)

    logger.info("regime_adjustments_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()