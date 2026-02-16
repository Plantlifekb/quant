#!/usr/bin/env python3
"""
Repair script for strategy_returns.parquet

This file contains:
- date
- strategy
- total_return  (weekly return)
- cum           (file cumulative, currently incorrect)

This script:
- Recomputes cumulative from total_return
- Compares file cum vs recomputed
- Writes a corrected file with cum fixed
"""

from pathlib import Path
import pandas as pd

SRC = Path(r"C:\Quant\data\analytics\strategy_returns.parquet")

def main():
    print(f"[INFO] Loading: {SRC}")
    df = pd.read_parquet(SRC)

    # Validate required columns
    required = {"date", "strategy", "total_return"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date").reset_index(drop=True)

    # Canonical recompute
    df["cum_recomputed"] = (1 + df["total_return"].fillna(0)).cumprod()

    # Compare with file cum (if present)
    if "cum" in df.columns:
        file_last = df["cum"].dropna().iloc[-1]
        recomputed_last = df["cum_recomputed"].iloc[-1]
        rel_diff = abs(file_last - recomputed_last) / max(1.0, abs(recomputed_last))

        print(f"[INFO] File last cum:       {file_last:.6f}")
        print(f"[INFO] Recomputed last cum: {recomputed_last:.6f}")
        print(f"[INFO] Relative diff:       {rel_diff*100:.2f}%")

        # Find first divergence
        diff_series = (df["cum"] - df["cum_recomputed"]).abs()
        diverge_idx = diff_series.gt(1e-6)
        if diverge_idx.any():
            first_bad = diff_series[diverge_idx].index[0]
            print("\n[INFO] First divergence at:")
            print(df.loc[first_bad, ["date", "total_return", "cum", "cum_recomputed"]])
        else:
            print("[INFO] No significant divergence found.")

    # Replace file cum with recomputed
    df["cum"] = df["cum_recomputed"]

    # Write fixed file
    dst = SRC.with_name(SRC.stem + "_fixed.parquet")
    df.to_parquet(dst)
    print(f"\n[OK] Wrote corrected file to:\n    {dst}")


if __name__ == "__main__":
    main()