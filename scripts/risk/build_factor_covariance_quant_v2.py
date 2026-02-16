"""
Quant v2.0 — Factor Covariance Engine

Builds a rolling, shrunk factor covariance matrix from Quant v2.0 factor returns.

Input:
    C:\Quant\data\risk\factor_returns_quant_v2.parquet

Output:
    C:\Quant\data\risk\factor_covariance_quant_v2.parquet

Method:
    • Identify factor columns (SEC_*, IND_*, *_raw, *_neutral)
    • Use rolling window (default 63 days) of daily factor returns
    • Compute sample covariance
    • Apply simple shrinkage toward diagonal (Ledoit–Wolf style lite)
    • Flatten K×K covariance into columns: cov_<fi>_<fj>
"""

from pathlib import Path
import numpy as np
import pandas as pd

BASE = Path(r"C:\Quant")

FACTOR_RETURNS_PATH = BASE / "data" / "risk" / "factor_returns_quant_v2.parquet"
FACTOR_COV_OUT = BASE / "data" / "risk" / "factor_covariance_quant_v2.parquet"

ROLLING_WINDOW = 63
SHRINKAGE_ALPHA = 0.3  # 0 = pure sample, 1 = pure diagonal


def compute_shrunk_cov(returns: pd.DataFrame) -> pd.DataFrame:
    """
    Compute shrunk covariance matrix for given factor returns (T x K).
    Shrinkage target: diagonal matrix with sample variances.
    """
    X = returns.values
    # Sample covariance (row: time, col: factor)
    sample_cov = np.cov(X, rowvar=False)

    # Diagonal target
    diag = np.diag(np.diag(sample_cov))

    shrunk = (1 - SHRINKAGE_ALPHA) * sample_cov + SHRINKAGE_ALPHA * diag
    return pd.DataFrame(shrunk, index=returns.columns, columns=returns.columns)


def main():
    print("\n=== BUILDING FACTOR COVARIANCE (Quant v2.0) ===\n")

    if not FACTOR_RETURNS_PATH.exists():
        raise FileNotFoundError(f"Missing factor returns file: {FACTOR_RETURNS_PATH}")

    fr = pd.read_parquet(FACTOR_RETURNS_PATH)
    fr["date"] = pd.to_datetime(fr["date"])
    fr = fr.sort_values("date").reset_index(drop=True)

    # Identify factor columns
    factor_cols = [
        c
        for c in fr.columns
        if c != "date"
        and (
            c.startswith("SEC_")
            or c.startswith("IND_")
            or c.endswith("_raw")
            or c.endswith("_neutral")
        )
    ]

    if len(factor_cols) == 0:
        raise ValueError("No factor columns found in factor_returns_quant_v2.parquet")

    print(f"• Using {len(factor_cols)} factor columns for covariance.")

    rows = []
    dates = fr["date"].unique()

    for i in range(len(dates)):
        window_end = dates[i]
        window_start_idx = max(0, i - ROLLING_WINDOW + 1)
        window_dates = dates[window_start_idx : i + 1]

        window = fr[fr["date"].isin(window_dates)].copy()

        if len(window) < 10:
            # Not enough history yet
            continue

        # Drop rows with any NaNs in factor returns
        R = window[factor_cols].astype(float).dropna(axis=0, how="any")
        if len(R) < 10:
            continue

        cov_df = compute_shrunk_cov(R)

        row = {"date": window_end}
        for fi in factor_cols:
            for fj in factor_cols:
                row[f"cov_{fi}_{fj}"] = float(cov_df.loc[fi, fj])
        rows.append(row)

    if not rows:
        raise RuntimeError("No covariance matrices could be estimated; check factor returns coverage.")

    cov_out = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    FACTOR_COV_OUT.parent.mkdir(parents=True, exist_ok=True)
    cov_out.to_parquet(FACTOR_COV_OUT, index=False)

    print(f"\n✔ Wrote factor covariance to: {FACTOR_COV_OUT}")
    print("\n🎉 Factor Covariance Engine completed successfully (Quant v2.0).\n")


if __name__ == "__main__":
    main()