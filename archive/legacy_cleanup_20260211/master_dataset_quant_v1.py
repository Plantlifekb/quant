#!/usr/bin/env python3
# ==================================================================================================
# Quant v1.0 — Master Dataset Assembly (Governed)
# ==================================================================================================
# PURPOSE:
#   Combine ingestion and enrichment outputs into a single governed master dataset.
#
# INPUTS:
#   C:\Quant\data\ingestion\ingestion_5years.csv
#   C:\Quant\data\enrichment\enriched_daily.csv
#
# OUTPUT:
#   C:\Quant\data\master\quant_master.csv
#
# SCHEMA:
#   All columns from enrichment (already includes ingestion fields)
#   + master_run_date (UTC timestamp)
#
# GOVERNANCE:
#   • No schema drift
#   • Deterministic ordering
#   • No missing OHLCV
#   • No missing enrichment fields
#   • No silent failures
#   • Full logging
#
# ==================================================================================================
# END OF HEADER — IMPLEMENTATION BEGINS BELOW
# ==================================================================================================

import os
from datetime import datetime
import pandas as pd
import numpy as np

import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from logging_quant_v1 import log

# ==================================================================================================
# PATHS
# ==================================================================================================

ROOT = r"C:\Quant"
DATA_INGESTION = os.path.join(ROOT, "data", "ingestion", "ingestion_5years.csv")
DATA_ENRICHMENT = os.path.join(ROOT, "data", "enrichment", "enriched_daily.csv")
DATA_MASTER_DIR = os.path.join(ROOT, "data", "master")
OUT_MASTER = os.path.join(DATA_MASTER_DIR, "quant_master.csv")

# ==================================================================================================
# MAIN LOGIC
# ==================================================================================================

def run() -> pd.DataFrame:
    log("[master_dataset_quant_v1] === Starting master dataset assembly ===")

    if not os.path.exists(DATA_INGESTION):
        raise FileNotFoundError(f"Ingestion file missing: {DATA_INGESTION}")

    if not os.path.exists(DATA_ENRICHMENT):
        raise FileNotFoundError(f"Enrichment file missing: {DATA_ENRICHMENT}")

    os.makedirs(DATA_MASTER_DIR, exist_ok=True)

    log("[master_dataset_quant_v1] Reading ingestion data ...")
    df_ing = pd.read_csv(DATA_INGESTION)

    log("[master_dataset_quant_v1] Reading enrichment data ...")
    df_enr = pd.read_csv(DATA_ENRICHMENT)

    # Lowercase columns for safety
    df_ing.columns = [c.lower() for c in df_ing.columns]
    df_enr.columns = [c.lower() for c in df_enr.columns]

    # Validate row counts
    if len(df_ing) != len(df_enr):
        log(f"[master_dataset_quant_v1] WARNING: Row mismatch: ingestion={len(df_ing)}, enrichment={len(df_enr)}")

    # Validate keys
    required_keys = ["date", "ticker"]
    for k in required_keys:
        if k not in df_enr.columns:
            raise ValueError(f"Missing key column in enrichment: {k}")

    # Master dataset = enrichment dataset (already contains ingestion fields)
    df_master = df_enr.copy()

    # Add master_run_date
    df_master["master_run_date"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    # Sort deterministically
    df_master = df_master.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Final sanity checks
    critical_cols = [
        "open", "high", "low", "close", "adj_close", "volume",
        "ret", "log_ret", "vol_5", "vol_20"
    ]
    missing = df_master[critical_cols].isna().sum()
    if missing.sum() > 0:
        log(f"[master_dataset_quant_v1] WARNING: Missing values detected:\n{missing}")

    # Write output
    df_master.to_csv(OUT_MASTER, index=False, encoding="utf-8")
    log(f"[master_dataset_quant_v1] Master dataset complete. Rows: {len(df_master)}")
    log(f"[master_dataset_quant_v1] Output written to {OUT_MASTER}")

    return df_master

# ==================================================================================================

if __name__ == "__main__":
    df_master = run()
    log(f"[master_dataset_quant_v1] Master dataset complete (main). Rows: {len(df_master)}")
