"""
Quant v1.1 — risk_regime_quant_v1.py
Regime-aware factor risk model.

Input:
- quant_factor_returns_regime_v1.csv

Output:
- quant_risk_regime_v1.csv
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("risk_regime_quant_v1")

FACTOR_RET_REGIME_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factor_returns_regime_v1.csv"
OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_risk_regime_v1.csv"

WINDOW = 252


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def main() -> None:
    logger.info("Starting risk_regime_quant_v1 (v1.1).")

    fr = pd.read_csv(FACTOR_RET_REGIME_FILE)
    fr.columns = [c.lower() for c in fr.columns]

    required = {"date", "factor_name", "factor_return_regime", "regime_label"}
    missing = required - set(fr.columns)
    if missing:
        raise ValueError(f"quant_factor_returns_regime_v1.csv missing columns: {sorted(missing)}")

    fr["date"] = pd.to_datetime(fr["date"], utc=True)
    fr = fr.sort_values("date").reset_index(drop=True)

    rows: list[dict] = []

    for (regime, factor), grp in fr.groupby(["regime_label", "factor_name"]):
        grp = grp.sort_values("date").reset_index(drop=True)
        grp["var"] = grp["factor_return_regime"].rolling(WINDOW).var()

        last = grp.dropna(subset=["var"])
        if last.empty:
            continue

        last_row = last.iloc[-1]
        rows.append(
            {
                "date": last_row["date"],
                "factor_name": factor,
                "factor_var_regime": float(last_row["var"]),
                "regime_label": regime,
                "window_length": WINDOW,
                "risk_model_run_date": iso_now(),
            }
        )

    out = pd.DataFrame.from_records(rows)
    if out.empty:
        logger.warning("No regime-aware risk rows generated.")
    else:
        out = out.sort_values(["date", "factor_name", "regime_label"]).reset_index(drop=True)

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False, encoding="utf-8")

    logger.info("risk_regime_quant_v1 (v1.1) completed successfully.")


if __name__ == "__main__":
    main()