#!/usr/bin/env python3
"""
Audit and repair cum_return in the weekly portfolio file.

- Finds the same file the dashboard uses (first existing in CANDIDATE_PATHS).
- Recomputes cumulative from weekly_return.
- Compares file cum_return vs recomputed.
- Writes a fixed parquet with corrected cum_return (no in-place overwrite).
"""

from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(r"C:\Quant")

CANDIDATE_PATHS = [
    BASE / "data" / "analytics" / "strategy_returns.parquet",
    BASE / "data" / "analytics" / "portfolio_performance_quant_v1.parquet",
    BASE / "data" / "analytics" / "performance_quant_v2.parquet",
    BASE / "data" / "weekly_portfolio.parquet",
]


def find_weekly_portfolio() -> Path:
    for p in CANDIDATE_PATHS:
        if p.exists():
            print(f"[INFO] Using weekly_portfolio file: {p}")
            return p
    raise FileNotFoundError("No weekly_portfolio candidate file found.")


def recompute_cum(df: pd.DataFrame) -> pd.Series:
    if "weekly_return" not in df.columns:
        raise ValueError("weekly_return column not found in file.")
    r = pd.to_numeric(df["weekly_return"], errors="coerce").fillna(0.0)
    return (1 + r).cumprod()


def main():
    src_path = find_weekly_portfolio()
    df = pd.read_parquet(src_path)

    if "date" not in df.columns:
        raise ValueError("date column not found in weekly_portfolio file.")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date").reset_index(drop=True)

    # Recompute canonical cumulative
    df["cum_return_recomputed"] = recompute_cum(df)

    # Existing file cumulative (if any)
    file_cum = None
    if "cum_return" in df.columns:
        file_cum = pd.to_numeric(df["cum_return"], errors="coerce")
    elif "cum" in df.columns:
        file_cum = pd.to_numeric(df["cum"], errors="coerce")

    if file_cum is None:
        print("[WARN] No existing cum_return/cum column found; will create one from recomputed.")
        df["cum_return"] = df["cum_return_recomputed"]
    else:
        # Compare last values
        recomputed_last = df["cum_return_recomputed"].dropna().iloc[-1]
        file_last = file_cum.dropna().iloc[-1]

        rel_diff = abs(file_last - recomputed_last) / max(1.0, abs(recomputed_last))
        print(f"[INFO] Last file cum:       {file_last:.6f}")
        print(f"[INFO] Last recomputed cum: {recomputed_last:.6f}")
        print(f"[INFO] Relative diff:       {rel_diff*100:.2f}%")

        # Find first divergence point
        diff_series = (file_cum - df["cum_return_recomputed"]).abs()
        diverge_idx = diff_series.gt(1e-6)
        if diverge_idx.any():
            first_bad = diff_series[diverge_idx].index[0]
            print("\n[INFO] First divergence at:")
            print(df.loc[first_bad, ["date", "weekly_return"]])
            print(f"file cum:       {file_cum.iloc[first_bad]:.6f}")
            print(f"recomputed cum: {df['cum_return_recomputed'].iloc[first_bad]:.6f}")
        else:
            print("[INFO] No significant divergence found; file cum already matches recomputed within tolerance.")

        # Replace file cum with recomputed (canonical)
        df["cum_return"] = df["cum_return_recomputed"]

    # Write fixed file (no overwrite)
    dst_path = src_path.with_name(src_path.stem + "_fixed.parquet")
    df.to_parquet(dst_path)
    print(f"\n[OK] Wrote fixed file with canonical cum_return to:\n    {dst_path}")


if __name__ == "__main__":
    main()