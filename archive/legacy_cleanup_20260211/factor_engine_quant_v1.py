#!/usr/bin/env python3
# ==================================================================================================
# Quant v1.0 — Factor Engine (Hybrid Multi-Factor Model)
# ==================================================================================================
# PURPOSE:
#   Load master dataset, compute all governed signals from the registry,
#   and write quant_factors.csv with full provenance.
#
# GOVERNANCE:
#   • No filtering of universe
#   • No dropping rows
#   • No reindexing
#   • Deterministic ordering
#   • All signals aligned to df.index
#   • Full provenance fields
# ==================================================================================================

import os
import pandas as pd
from datetime import datetime

import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from logging_quant_v1 import log
from signal_library_quant_v1 import SIGNAL_REGISTRY

# ==================================================================================================
# PATHS
# ==================================================================================================

ROOT = r"C:\Quant"
MASTER = os.path.join(ROOT, "data", "master", "quant_master.csv")
OUT_FACTORS = os.path.join(ROOT, "data", "analytics", "quant_factors.csv")

# ==================================================================================================
# MAIN FACTOR ENGINE
# ==================================================================================================

def run() -> pd.DataFrame:
    log("[factor_engine_quant_v1] === Starting factor engine ===")

    if not os.path.exists(MASTER):
        raise FileNotFoundError("Master dataset missing.")

    df = pd.read_csv(MASTER)
    df.columns = [c.lower() for c in df.columns]

    log(f"[factor_engine_quant_v1] Loaded master dataset: {len(df)} rows, "
        f"{df['ticker'].nunique()} tickers")

    # Compute all signals in registry
    for name, meta in SIGNAL_REGISTRY.items():
        func = meta["func"]
        log(f"[factor_engine_quant_v1] Computing signal: {name}")
        try:
            df[name] = func(df)
        except Exception as e:
            log(f"[factor_engine_quant_v1] ERROR computing {name}: {e}")
            raise

    # Provenance
    df["factor_run_date"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    # Deterministic ordering
    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)

    # Write output
    df.to_csv(OUT_FACTORS, index=False, encoding="utf-8")
    log(f"[factor_engine_quant_v1] Factor dataset written: {OUT_FACTORS}")
    log(f"[factor_engine_quant_v1] Rows: {len(df)}")

    return df

# ==================================================================================================

if __name__ == "__main__":
    df = run()
    log(f"[factor_engine_quant_v1] Done. Rows: {len(df)}")
