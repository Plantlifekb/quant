"""
Quant v1.1 — attribution_regime_quant_v1.py
Regime-aware attribution aggregation.

Inputs:
- quant_attribution_daily_v1.csv
- quant_regime_states_v1.csv

Output:
- quant_attribution_regime_v1.csv
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("attribution_regime_quant_v1")

ATTRIB_DAILY_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_attribution_daily_v1_1.csv"
REGIME_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_regime_states_v1.csv"
OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_attribution_regime_v1.csv"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def main() -> None:
    logger.info("Starting attribution_regime_quant_v1 (v1.1).")

    a = pd.read_csv(ATTRIB_DAILY_FILE)
    a.columns = [c.lower() for c in a.columns]
    a["date"] = pd.to_datetime(a["date"], utc=True)

    required = {"date", "ticker", "factor_name", "contribution"}
    missing = required - set(a.columns)
    if missing:
        raise ValueError(f"quant_attribution_daily_v1.csv missing columns: {sorted(missing)}")

    reg = pd.read_csv(REGIME_FILE)
    reg.columns = [c.lower() for c in reg.columns]
    reg["date"] = pd.to_datetime(reg["date"], utc=True)
    reg = reg.sort_values("date").reset_index(drop=True)

    a = a.sort_values("date")
    a = pd.merge_asof(
        a,
        reg[["date", "regime_label"]].sort_values("date"),
        on="date",
        direction="backward",
    )
    a["regime_label"] = a["regime_label"].fillna("unknown")

    out = (
        a.groupby(["regime_label", "factor_name"], as_index=False)["contribution"]
        .sum()
        .sort_values(["regime_label", "factor_name"])
        .reset_index(drop=True)
    )

    out["attribution_regime_run_date"] = iso_now()

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False, encoding="utf-8")

    logger.info("attribution_regime_quant_v1 (v1.1) completed successfully.")


if __name__ == "__main__":
    main()