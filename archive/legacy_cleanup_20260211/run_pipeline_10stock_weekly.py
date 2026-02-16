"""
Module: run_pipeline_10stock_weekly.py
Quant variant: 10-stock weekly strategy

Purpose:
    Execute the full governed pipeline in correct order:
        1. Ingestion
        2. Enrichment
        3. Ranking
        4. Backtest
        5. Dashboard

This script does NOT contain business logic.
It simply calls the existing modules in sequence.
"""

import subprocess
import os

BASE = r"C:\Quant\scripts"

STEPS = [
    r"ingestion\ingestion_5years_quant_v1.py",
    r"enrichment\enrichment_10stock_weekly.py",
    r"ranking\ranking_10stock_weekly.py",
    r"backtest\backtest_10stock_weekly.py",
    r"dashboard\dashboard_10stock_weekly.py",
]

def run_step(script_path):
    full_path = os.path.join(BASE, script_path)
    print(f"Running: {full_path}")
    subprocess.run(["python", full_path], check=True)

def main():
    for step in STEPS:
        run_step(step)

if __name__ == "__main__":
    main()