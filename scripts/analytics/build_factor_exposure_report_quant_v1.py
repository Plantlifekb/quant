"""
Quant v1.0 — Factor exposure reporting (no factor model yet)

Since the current risk_model has no factor exposures,
we generate a governed zero-exposure report.

Builds:
    C:\Quant\data\analytics\factor_exposure_report_quant_v1.parquet
"""

from pathlib import Path
import pandas as pd

BASE = Path(r"C:\Quant")
DATA_ANALYTICS = BASE / "data" / "analytics"

OPT = DATA_ANALYTICS / "optimiser_regime_quant_v1.parquet"
OUT = DATA_ANALYTICS / "factor_exposure_report_quant_v1.parquet"


def main():
    print("\n=== BUILDING FACTOR EXPOSURE REPORT (Quant v1.0 — zero exposure) ===\n")

    opt = pd.read_parquet(OPT)
    opt["date"] = pd.to_datetime(opt["date"])

    dates = sorted(opt["date"].unique())

    # Zero-exposure placeholder
    out = pd.DataFrame({
        "date": dates,
        "factor_1": 0.0,
        "factor_2": 0.0,
        "factor_3": 0.0,
    })

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)

    print(f"✔ Wrote factor_exposure_report_quant_v1.parquet → {OUT}\n")
    print("🎉 Factor exposure reporting built successfully (zero-exposure placeholder).\n")


if __name__ == "__main__":
    main()