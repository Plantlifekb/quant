"""
Quant v2.0 — Factor Returns Engine

Computes daily factor returns by cross-sectional regression of realised returns
on the Quant v2.0 factor exposures.

Inputs:
    C:\Quant\data\risk\factor_exposures_quant_v2.parquet
    C:\Quant\data\ingestion\fundamentals.parquet   (for realised returns)

Output:
    C:\Quant\data\risk\factor_returns_quant_v2.parquet

Notes:
    • Uses cross-sectional OLS per date: ret ~ factors
    • Includes:
        - sector factors (SEC_*)
        - industry factors (IND_*)
        - style factors (raw + neutral)
    • Applies basic guards against singular matrices and small cross-sections.
"""

from pathlib import Path
import numpy as np
import pandas as pd

BASE = Path(r"C:\Quant")

EXPOSURES_PATH = BASE / "data" / "risk" / "factor_exposures_quant_v2.parquet"
FUNDAMENTALS_PATH = BASE / "data" / "ingestion" / "fundamentals.parquet"
FACTOR_RETURNS_OUT = BASE / "data" / "risk" / "factor_returns_quant_v2.parquet"


def _ols_factor_returns(X: np.ndarray, y: np.ndarray) -> np.ndarray:
    """
    Simple OLS: beta = (X'X)^(-1) X'y
    Uses pseudo-inverse for robustness.
    """
    # Add tiny ridge for numerical stability
    xtx = X.T @ X
    k = xtx.shape[0]
    xtx += 1e-8 * np.eye(k)
    beta = np.linalg.pinv(xtx) @ (X.T @ y)
    return beta


def main():
    print("\n=== BUILDING FACTOR RETURNS (Quant v2.0) ===\n")

    if not EXPOSURES_PATH.exists():
        raise FileNotFoundError(f"Missing factor exposures file: {EXPOSURES_PATH}")
    if not FUNDAMENTALS_PATH.exists():
        raise FileNotFoundError(f"Missing fundamentals file: {FUNDAMENTALS_PATH}")

    exp = pd.read_parquet(EXPOSURES_PATH)
    exp["date"] = pd.to_datetime(exp["date"])
    exp["ticker"] = exp["ticker"].astype(str).str.upper()

    fnd = pd.read_parquet(FUNDAMENTALS_PATH)
    fnd["date"] = pd.to_datetime(fnd["date"])
    fnd["ticker"] = fnd["ticker"].astype(str).str.upper()

    # Use realised daily return as dependent variable
    if "ret" not in fnd.columns:
        raise ValueError("fundamentals.parquet must contain 'ret' column for realised returns")
    fnd["ret"] = fnd["ret"].astype(float)

    # Merge returns onto exposures
    print("• Merging realised returns with factor exposures ...")
    df = (
        exp.merge(
            fnd[["date", "ticker", "ret"]],
            on=["date", "ticker"],
            how="inner",
        )
        .dropna(subset=["ret"])
        .copy()
    )

    # Identify factor columns
    factor_cols = [
        c
        for c in df.columns
        if c.startswith("SEC_")
        or c.startswith("IND_")
        or c.endswith("_raw")
        or c.endswith("_neutral")
    ]

    if len(factor_cols) == 0:
        raise ValueError("No factor columns found in exposures (SEC_*, IND_*, *_raw, *_neutral).")

    print(f"• Using {len(factor_cols)} factor columns.")

    rows = []
    for date, g in df.groupby("date"):
        g = g.copy()

        # Require a minimum cross-section size
        if len(g) < len(factor_cols) + 5:
            # Too few names to estimate a stable cross-section
            continue

        X = g[factor_cols].astype(float).values
        y = g["ret"].astype(float).values

        # Drop rows with any NaNs in X or y
        mask = np.isfinite(X).all(axis=1) & np.isfinite(y)
        X = X[mask]
        y = y[mask]

        if X.shape[0] < len(factor_cols) + 5:
            continue

        try:
            beta = _ols_factor_returns(X, y)
        except Exception as e:
            print(f"  ! Skipping date {date.date()} due to regression error: {e}")
            continue

        row = {"date": date}
        for col, b in zip(factor_cols, beta):
            row[col] = float(b)
        rows.append(row)

    if not rows:
        raise RuntimeError("No factor returns could be estimated; check data coverage and factor design.")

    fr = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    FACTOR_RETURNS_OUT.parent.mkdir(parents=True, exist_ok=True)
    fr.to_parquet(FACTOR_RETURNS_OUT, index=False)

    print(f"\n✔ Wrote factor returns to: {FACTOR_RETURNS_OUT}")
    print("\n🎉 Factor Returns Engine completed successfully (Quant v2.0).\n")


if __name__ == "__main__":
    main()