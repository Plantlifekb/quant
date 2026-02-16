"""
reporting_engine_v1.py
Quant v1.1 — Reporting Engine
------------------------------------------------------------

Consumes:
    - C:\Quant\data\analytics\summary\quant_summary_v1.csv
    - C:\Quant\data\analytics\risk\quant_risk_daily_v1.csv
    - C:\Quant\data\analytics\risk\quant_risk_model_v1.csv

Produces:
    - C:\Quant\data\analytics\reporting\quant_report_v1.csv
    - C:\Quant\data\analytics\reporting\quant_dashboard_inputs_v1.csv
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import logging
import numpy as np

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
LOG_DIR = Path(r"C:\Quant\logs\reporting_engine")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "reporting_engine_v1.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("reporting_engine_v1")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ------------------------------------------------------------
# Input paths
# ------------------------------------------------------------
SUMMARY_FILE = Path(r"C:\Quant\data\analytics\summary\quant_summary_v1.csv")
RISK_DAILY_FILE = Path(r"C:\Quant\data\analytics\risk\quant_risk_daily_v1.csv")
RISK_MODEL_FILE = Path(r"C:\Quant\data\analytics\risk\quant_risk_model_v1.csv")

# ------------------------------------------------------------
# Output paths
# ------------------------------------------------------------
REPORT_DIR = Path(r"C:\Quant\data\analytics\reporting")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_STACKED_FILE = REPORT_DIR / "quant_report_v1.csv"
REPORT_DASHBOARD_FILE = REPORT_DIR / "quant_dashboard_inputs_v1.csv"


# ------------------------------------------------------------
# Helpers
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
# Build stacked, sectioned report
# ------------------------------------------------------------
def build_stacked_report(
    df_summary: pd.DataFrame,
    df_risk_daily: pd.DataFrame,
    df_risk_model: pd.DataFrame,
) -> pd.DataFrame:
    sections = []

    # PNL summary section (from summary)
    if not df_summary.empty and "SECTION" in df_summary.columns:
        pnl_mask = df_summary["SECTION"].isin(
            ["PNL_SUMMARY", "SUMMARY_PNL", "DAILY_PNL_SUMMARY"]
        )
        df_pnl = df_summary[pnl_mask].copy()
        if not df_pnl.empty:
            df_pnl.insert(0, "REPORT_SECTION", "REPORT_PNL_SUMMARY")
            sections.append(df_pnl)
        else:
            logger.warning("No PNL summary section found in summary file.")
    else:
        logger.warning("Summary file empty or missing SECTION column; skipping PNL summary.")

    # Regime summary section (from summary)
    if not df_summary.empty and "SECTION" in df_summary.columns:
        reg_mask = df_summary["SECTION"].isin(
            ["REGIME_SUMMARY", "SUMMARY_REGIME"]
        )
        df_reg = df_summary[reg_mask].copy()
        if not df_reg.empty:
            df_reg.insert(0, "REPORT_SECTION", "REPORT_REGIME_SUMMARY")
            sections.append(df_reg)
        else:
            logger.info("No explicit regime summary section found in summary file.")
    else:
        logger.warning("Summary file empty or missing SECTION column; skipping regime summary.")

    # Rolling attribution summary (from summary rolling section if present)
    if not df_summary.empty and "SECTION" in df_summary.columns:
        roll_mask = df_summary["SECTION"].isin(
            ["ROLLING_SUMMARY", "SUMMARY_ROLLING"]
        )
        df_attr_roll = df_summary[roll_mask].copy()
        if not df_attr_roll.empty:
            df_attr_roll.insert(0, "REPORT_SECTION", "REPORT_ATTRIBUTION_ROLLING")
            sections.append(df_attr_roll)
        else:
            logger.info("No rolling attribution summary section found in summary file.")
    else:
        logger.warning("Summary file empty or missing SECTION column; skipping rolling attribution summary.")

    # Risk daily section
    if not df_risk_daily.empty:
        df_rd = df_risk_daily.copy()
        # Ensure SECTION exists but keep original as RISK_DAILY
        if "SECTION" in df_rd.columns:
            df_rd.insert(0, "REPORT_SECTION", "REPORT_RISK_DAILY")
        else:
            df_rd.insert(0, "REPORT_SECTION", "REPORT_RISK_DAILY")
        sections.append(df_rd)
    else:
        logger.warning("Risk daily file empty; skipping REPORT_RISK_DAILY section.")

    # Risk rolling section (from risk model)
    if not df_risk_model.empty:
        df_rm = df_risk_model.copy()
        if "SECTION" in df_rm.columns:
            df_rm.insert(0, "REPORT_SECTION", "REPORT_RISK_ROLLING")
        else:
            df_rm.insert(0, "REPORT_SECTION", "REPORT_RISK_ROLLING")
        sections.append(df_rm)
    else:
        logger.warning("Risk model file empty; skipping REPORT_RISK_ROLLING section.")

    if not sections:
        logger.warning("No sections assembled for stacked report; returning empty DataFrame.")
        return pd.DataFrame()

    # Align columns across sections
    all_cols = set()
    for df in sections:
        all_cols.update(df.columns)

    all_cols = list(all_cols)
    aligned_sections = []
    for df in sections:
        aligned_sections.append(df.reindex(columns=all_cols))

    stacked = pd.concat(aligned_sections, ignore_index=True)
    return stacked


# ------------------------------------------------------------
# Build flattened dashboard dataset
# ------------------------------------------------------------
def build_dashboard_inputs(
    df_summary: pd.DataFrame,
    df_risk_daily: pd.DataFrame,
    df_risk_model: pd.DataFrame,
) -> pd.DataFrame:
    if df_risk_daily.empty:
        logger.warning("Risk daily file empty; dashboard dataset will be empty.")
        return pd.DataFrame()

    df_rd = df_risk_daily.copy()
    # Normalize date
    if "date" in df_rd.columns:
        df_rd["date"] = pd.to_datetime(df_rd["date"], errors="coerce").dt.date

    # Start with risk daily metrics
    dash = df_rd[[
        c for c in df_rd.columns
        if c in [
            "date",
            "daily_volatility",
            "factor_risk",
            "beta",
            "liquidity_risk",
            "turnover_risk",
            "regime_risk",
        ]
    ]].copy()

    # Attach PNL per date from summary if available
    pnl_daily = pd.DataFrame()
    if not df_summary.empty and "SECTION" in df_summary.columns:
        # Try to find a daily summary section
        daily_mask = df_summary["SECTION"].isin(
            ["DAILY_SUMMARY", "PNL_DAILY", "SUMMARY_DAILY"]
        )
        pnl_daily = df_summary[daily_mask].copy()

        # Heuristic: expect a 'date' column and some pnl column
        if not pnl_daily.empty and "date" in pnl_daily.columns:
            pnl_daily["date"] = pd.to_datetime(pnl_daily["date"], errors="coerce").dt.date
            # Try to find a pnl column
            pnl_cols = [c for c in pnl_daily.columns if "pnl" in c.lower()]
            if pnl_cols:
                pnl_col = pnl_cols[0]
                pnl_daily = pnl_daily[["date", pnl_col]].rename(columns={pnl_col: "pnl"})
                dash = dash.merge(pnl_daily, on="date", how="left")
            else:
                logger.info("No PNL column detected in daily summary section; dashboard will omit PNL.")
        else:
            logger.info("No usable daily summary section with date column; dashboard will omit PNL.")
    else:
        logger.info("Summary file empty or missing SECTION; dashboard will omit PNL.")

    # Aggregate rolling risk model per date for dashboard
    if not df_risk_model.empty and "date" in df_risk_model.columns:
        df_rm = df_risk_model.copy()
        df_rm["date"] = pd.to_datetime(df_rm["date"], errors="coerce").dt.date

        agg = df_rm.groupby("date").agg(
            rolling_volatility_mean=("rolling_volatility", "mean"),
            rolling_factor_risk_mean=("rolling_factor_risk", "mean"),
            rolling_beta_mean=("rolling_beta", "mean"),
            rolling_turnover_pressure_mean=("rolling_turnover_pressure", "mean"),
            rolling_liquidity_pressure_mean=("rolling_liquidity_pressure", "mean"),
        ).reset_index()

        dash = dash.merge(agg, on="date", how="left")
    else:
        logger.info("Risk model file empty or missing date; dashboard will omit rolling aggregates.")

    # Order columns for readability
    preferred_order = [
        "date",
        "pnl",
        "daily_volatility",
        "factor_risk",
        "beta",
        "liquidity_risk",
        "turnover_risk",
        "regime_risk",
        "rolling_volatility_mean",
        "rolling_factor_risk_mean",
        "rolling_beta_mean",
        "rolling_turnover_pressure_mean",
        "rolling_liquidity_pressure_mean",
    ]
    cols = [c for c in preferred_order if c in dash.columns] + [
        c for c in dash.columns if c not in preferred_order
    ]
    dash = dash[cols]

    return dash


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    logger.info("------------------------------------------------------------")
    logger.info("Starting Reporting Engine v1.1")
    logger.info(f"Run timestamp: {iso_now()}")
    logger.info("------------------------------------------------------------")

    df_summary = load_csv(SUMMARY_FILE)
    df_risk_daily = load_csv(RISK_DAILY_FILE)
    df_risk_model = load_csv(RISK_MODEL_FILE)

    if df_summary.empty:
        logger.warning("Summary file is empty or missing.")
    if df_risk_daily.empty:
        logger.warning("Risk daily file is empty or missing.")
    if df_risk_model.empty:
        logger.warning("Risk model file is empty or missing.")

    # 1) Stacked, sectioned report
    df_report = build_stacked_report(df_summary, df_risk_daily, df_risk_model)
    if not df_report.empty:
        df_report.to_csv(REPORT_STACKED_FILE, index=False)
        logger.info(f"Stacked report written to {REPORT_STACKED_FILE}")
    else:
        logger.warning("Stacked report is empty; no file written.")

    # 2) Flattened dashboard dataset
    df_dash = build_dashboard_inputs(df_summary, df_risk_daily, df_risk_model)
    if not df_dash.empty:
        df_dash.to_csv(REPORT_DASHBOARD_FILE, index=False)
        logger.info(f"Dashboard inputs written to {REPORT_DASHBOARD_FILE}")
    else:
        logger.warning("Dashboard dataset is empty; no file written.")

    logger.info("Reporting Engine v1.1 completed.")
    logger.info("------------------------------------------------------------")


if __name__ == "__main__":
    main()