"""
Module: ranking_10stock_weekly.py
Quant variant: 10-stock weekly strategy

Purpose:
    Select the top 10 tickers every Monday using the enrichment output.
    Ranking is deterministic:
        - highest score first
        - ties broken alphabetically
    All selected tickers receive equal weight (0.10).

Inputs:
    C:\Quant\data\enriched\enriched_10stock_weekly.csv
        Required columns:
            date
            ticker
            score
            monday_flag

Outputs:
    C:\Quant\data\signals\weekly_selection.csv
        Columns:
            date
            ticker
            score
            weight

Governance:
    - No schema drift
    - Deterministic ranking
    - Equal weights
    - No writing outside governed paths
"""

import os
import pandas as pd

INPUT_PATH = r"C:\Quant\data\enriched\enriched_10stock_weekly.csv"
OUTPUT_DIR = r"C:\Quant\data\signals"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "weekly_selection.csv")


def load_enriched(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    required = {"date", "ticker", "score", "monday_flag"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in enriched file: {missing}")

    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")

    # Deterministic sort
    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)
    return df


def select_top10(df: pd.DataFrame) -> pd.DataFrame:
    # Filter to Mondays only
    mondays = df[df["monday_flag"] == True].copy()

    if mondays.empty:
        raise ValueError("No Monday rows found in enriched dataset.")

    selections = []

    # Process each Monday independently
    for date, group in mondays.groupby("date"):
        # Rank by score (descending), then ticker (ascending)
        ranked = group.sort_values(
            by=["score", "ticker"],
            ascending=[False, True]
        ).head(10)

        ranked["weight"] = 0.10
        selections.append(ranked[["date", "ticker", "score", "weight"]])

    out = pd.concat(selections, ignore_index=True)

    # Deterministic final sort
    out = out.sort_values(["date", "ticker"]).reset_index(drop=True)
    return out


def main():
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Enriched file not found: {INPUT_PATH}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df_enriched = load_enriched(INPUT_PATH)
    df_selection = select_top10(df_enriched)

    df_selection.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")


if __name__ == "__main__":
    main()