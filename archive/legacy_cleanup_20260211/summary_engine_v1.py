"""
summary_engine_v1.py
Quant v1.0 — Summary Engine
------------------------------------------------------------

Consumes attribution outputs and produces a governed summary dataset:

    C:\Quant\data\analytics\summary\quant_summary_v1.csv

Sections produced:
    - PNL_SUMMARY
    - TURNOVER_SUMMARY
    - LIQUIDITY_SUMMARY
    - REGIME_SUMMARY
    - ROLLING_SUMMARY

This file is the canonical input for:
    - risk_engine_v1.py
    - reporting_engine_v1.py
    - dashboard_v1.py
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import logging

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
LOG_DIR = Path(r"C:\Quant\logs\summary_engine")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "summary_engine_v1.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("summary_engine_v1")


def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ------------------------------------------------------------
# Input paths (governed)
# ------------------------------------------------------------
ATTR_DIR = Path(r"C:\Quant\data\analytics\attribution_outputs_v1")

FILES = {
    "sector": ATTR_DIR / "attribution_sector.csv",
    "regime": ATTR_DIR / "attribution_regime_summary.csv",
    "rolling": ATTR_DIR / "attribution_rolling.csv",
    "liquidity": ATTR_DIR / "liquidity_costs_enhanced.csv",
    "expected_vs_realised": ATTR_DIR / "attribution_expected_vs_realised.csv",
}

# ------------------------------------------------------------
# Output path (governed)
# ------------------------------------------------------------
OUT_DIR = Path(r"C:\Quant\data\analytics\summary")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "quant_summary_v1.csv"


# ------------------------------------------------------------
# Load helper
# ------------------------------------------------------------
def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        logger.warning(f"Missing input file: {path}")
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        logger.error(f"Failed to load {path}: {e}")
        return pd.DataFrame()


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    logger.info("------------------------------------------------------------")
    logger.info("Starting Summary Engine v1.0")
    logger.info(f"Run timestamp: {iso_now()}")
    logger.info("------------------------------------------------------------")

    # Load inputs
    df_sector = load_csv(FILES["sector"])
    df_regime = load_csv(FILES["regime"])
    df_rolling = load_csv(FILES["rolling"])
    df_liq = load_csv(FILES["liquidity"])
    df_evr = load_csv(FILES["expected_vs_realised"])

    sections = []

    # ------------------------------------------------------------
    # PNL SUMMARY (sector-level PnL)
    # ------------------------------------------------------------
    if not df_sector.empty:
        df_pnl = pd.DataFrame({
            "SECTION": ["PNL_SUMMARY"],
            "total_pnl": [df_sector["total_pnl"].sum()],
            "num_sectors": [df_sector["market_sector"].nunique()],
            "avg_gross_weight_mean": [df_sector["avg_gross_weight"].mean()],
        })
        sections.append(df_pnl)

    # ------------------------------------------------------------
    # TURNOVER SUMMARY (from expected_vs_realised if available)
    # ------------------------------------------------------------
    if not df_evr.empty and "turnover_cost" in df_evr.columns:
        df_turn = pd.DataFrame({
            "SECTION": ["TURNOVER_SUMMARY"],
            "turnover_cost_total": [df_evr["turnover_cost"].sum()],
            "turnover_cost_mean": [df_evr["turnover_cost"].mean()],
        })
        sections.append(df_turn)

    # ------------------------------------------------------------
    # LIQUIDITY SUMMARY
    # ------------------------------------------------------------
    if not df_liq.empty and "liquidity_cost" in df_liq.columns:
        df_liq_sum = pd.DataFrame({
            "SECTION": ["LIQUIDITY_SUMMARY"],
            "liquidity_cost_total": [df_liq["liquidity_cost"].sum()],
            "liquidity_cost_mean": [df_liq["liquidity_cost"].mean()],
        })
        sections.append(df_liq_sum)

    # ------------------------------------------------------------
    # REGIME SUMMARY (patched to match your schema)
    # ------------------------------------------------------------
    if not df_regime.empty:

        df_regime = df_regime.rename(columns={
            "regime_label": "regime",
            "net_contribution": "pnl"
        })

        df_regime["contribution"] = (
            df_regime.get("factor_contribution", 0)
            + df_regime.get("idiosyncratic_contribution", 0)
        )

        df_regime_grouped = df_regime.groupby("regime").agg({
            "pnl": "sum",
            "contribution": "sum"
        }).reset_index()

        df_regime_grouped.insert(0, "SECTION", "REGIME_SUMMARY")
        sections.append(df_regime_grouped)

    # ------------------------------------------------------------
    # ROLLING SUMMARY
    # ------------------------------------------------------------
    if not df_rolling.empty:
        df_roll = df_rolling.copy()
        df_roll.insert(0, "SECTION", "ROLLING_SUMMARY")
        sections.append(df_roll)

    # ------------------------------------------------------------
    # Combine & write output
    # ------------------------------------------------------------
    if sections:
        df_out = pd.concat(sections, ignore_index=True)
        df_out.to_csv(OUT_FILE, index=False)
        logger.info(f"Summary written to {OUT_FILE}")
    else:
        logger.warning("No sections produced — summary file not written.")

    logger.info("Summary Engine v1.0 completed.")
    logger.info("------------------------------------------------------------")


if __name__ == "__main__":
    main()