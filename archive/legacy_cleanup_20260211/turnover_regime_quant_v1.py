"""
Quant v1.1 — turnover_regime_quant_v1.py
Regime-aware turnover diagnostics.

Inputs:
- quant_portfolio_weights_tradable_v1_osqp.csv
- quant_regime_states_v1.csv

Output:
- quant_turnover_regime_v1.csv

Governance:
- Lowercase columns
- UTC timestamps
- Deterministic behaviour
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("turnover_regime_quant_v1")

WEIGHTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_tradable_v1_osqp.csv"
REGIME_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_regime_states_v1.csv"
OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_turnover_regime_v1.csv"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def main() -> None:
    logger.info("Starting turnover_regime_quant_v1 (v1.1).")

    # -----------------------------------------------------
    # Load weights
    # -----------------------------------------------------
    w = pd.read_csv(WEIGHTS_FILE)
    w.columns = [c.lower() for c in w.columns]
    w["date"] = pd.to_datetime(w["date"], utc=True)
    w = w.sort_values(["date", "ticker"]).reset_index(drop=True)

    if "weight_tradable_v1" not in w.columns:
        raise ValueError("weights file missing 'weight_tradable_v1' column.")

    # -----------------------------------------------------
    # Compute daily turnover
    # -----------------------------------------------------
    w["weight_prev"] = w.groupby("ticker")["weight_tradable_v1"].shift(1).fillna(0.0)
    w["turnover_abs"] = (w["weight_tradable_v1"] - w["weight_prev"]).abs()

    daily_to = (
        w.groupby("date", as_index=False)["turnover_abs"]
        .sum()
        .rename(columns={"turnover_abs": "turnover"})
    )

    # -----------------------------------------------------
    # Load regimes and merge
    # -----------------------------------------------------
    reg = pd.read_csv(REGIME_FILE)
    reg.columns = [c.lower() for c in reg.columns]
    reg["date"] = pd.to_datetime(reg["date"], utc=True)
    reg = reg.sort_values("date").reset_index(drop=True)

    daily_to = daily_to.sort_values("date")
    daily_to = pd.merge_asof(
        daily_to,
        reg[["date", "regime_label"]].sort_values("date"),
        on="date",
        direction="backward",
    )
    daily_to["regime_label"] = daily_to["regime_label"].fillna("unknown")

    # -----------------------------------------------------
    # Aggregate turnover by regime (pandas 2.x compatible)
    # -----------------------------------------------------
    out = (
        daily_to.groupby("regime_label", as_index=False)
        .agg({"turnover": "mean"})
        .rename(columns={"turnover": "avg_turnover"})
        .sort_values("regime_label")
        .reset_index(drop=True)
    )

    out["turnover_regime_run_date"] = iso_now()

    # -----------------------------------------------------
    # Save
    # -----------------------------------------------------
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False, encoding="utf-8")

    logger.info("turnover_regime_quant_v1 (v1.1) completed successfully.")


if __name__ == "__main__":
    main()