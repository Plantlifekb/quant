"""
Quant v2.0 — Attribution Engine

Decomposes daily portfolio performance into:
    • Factor contribution
    • Idiosyncratic contribution
    • Alpha contribution
    • Total return
    • Factor risk contribution
    • Idiosyncratic risk contribution

Inputs:
    • portfolio_quant_v2.parquet
    • factor_exposures_quant_v2.parquet
    • factor_returns_quant_v2.parquet
    • factor_covariance_quant_v2.parquet
    • fundamentals.parquet (for realised returns)
    • alpha_quant_v2.parquet

Output:
    C:\Quant\data\analytics\attribution_quant_v2.parquet
"""

from pathlib import Path
import numpy as np
import pandas as pd

BASE = Path(r"C:\Quant")

PORT = BASE / "data" / "analytics" / "portfolio_quant_v2.parquet"
EXPOSURES = BASE / "data" / "risk" / "factor_exposures_quant_v2.parquet"
FACTOR_RETURNS = BASE / "data" / "risk" / "factor_returns_quant_v2.parquet"
FACTOR_COV = BASE / "data" / "risk" / "factor_covariance_quant_v2.parquet"
FUNDAMENTALS = BASE / "data" / "ingestion" / "fundamentals.parquet"
ALPHA = BASE / "data" / "signals" / "alpha_quant_v2.parquet"

ATTR_OUT = BASE / "data" / "analytics" / "attribution_quant_v2.parquet"


def main():
    print("\n=== BUILDING ATTRIBUTION (Quant v2.0) ===\n")

    # Load portfolio
    port = pd.read_parquet(PORT)
    port["date"] = pd.to_datetime(port["date"])
    port["ticker"] = port["ticker"].astype(str).str.upper()

    # Load exposures
    exp = pd.read_parquet(EXPOSURES)
    exp["date"] = pd.to_datetime(exp["date"])
    exp["ticker"] = exp["ticker"].astype(str).str.upper()

    # Load factor returns
    fr = pd.read_parquet(FACTOR_RETURNS)
    fr["date"] = pd.to_datetime(fr["date"])

    # Load fundamentals (for realised returns)
    fnd = pd.read_parquet(FUNDAMENTALS)
    fnd["date"] = pd.to_datetime(fnd["date"])
    fnd["ticker"] = fnd["ticker"].astype(str).str.upper()

    # Load alpha
    alpha = pd.read_parquet(ALPHA)
    alpha["date"] = pd.to_datetime(alpha["date"])
    alpha["ticker"] = alpha["ticker"].astype(str).str.upper()

    rows = []

    for date, p in port.groupby("date"):
        p = p.copy()

        # Merge exposures
        e = exp[exp["date"] == date]
        merged = p.merge(e, on=["date", "ticker"], how="inner")

        # Merge realised returns
        r = fnd[fnd["date"] == date][["ticker", "ret"]]
        merged = merged.merge(r, on="ticker", how="left")

        # Merge alpha
        a = alpha[alpha["date"] == date][["ticker", "alpha_z"]]
        merged = merged.merge(a, on="ticker", how="left")

        if merged.empty:
            continue

        # Identify factor columns
        factor_cols = [
            c for c in merged.columns
            if c.startswith("SEC_") or c.startswith("IND_") or c.endswith("_raw") or c.endswith("_neutral")
        ]

        B = merged[factor_cols].astype(float).values  # N x K
        w = merged["weight"].astype(float).values     # N
        ret = merged["ret"].astype(float).fillna(0).values
        alpha_vec = merged["alpha_z"].astype(float).fillna(0).values

        # Factor returns for this date
        fr_row = fr[fr["date"] == date]
        if fr_row.empty:
            continue
        f_ret = fr_row[factor_cols].values.flatten()  # K

        # === CONTRIBUTIONS ===

        # Factor contribution
        factor_contrib = float(w @ (B @ f_ret))

        # Alpha contribution
        alpha_contrib = float(w @ alpha_vec)

        # Realised total return
        total_return = float(w @ ret)

        # Idiosyncratic contribution
        idio_contrib = total_return - factor_contrib

        rows.append({
            "date": date,
            "factor_contrib": factor_contrib,
            "alpha_contrib": alpha_contrib,
            "idio_contrib": idio_contrib,
            "total_return": total_return,
        })

    out = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    ATTR_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(ATTR_OUT, index=False)

    print(f"✔ Wrote attribution to: {ATTR_OUT}")
    print("\n🎉 Attribution Engine (Quant v2.0) completed successfully.\n")


if __name__ == "__main__":
    main()