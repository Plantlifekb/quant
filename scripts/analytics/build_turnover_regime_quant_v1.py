"""
Quant v1.0 — Turnover by regime reporting (governed, no NaNs)

Builds:
    C:\Quant\data\analytics\turnover_regime_quant_v1.parquet
"""

from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(r"C:\Quant")
DATA_ANALYTICS = BASE / "data" / "analytics"

OPT = DATA_ANALYTICS / "optimiser_regime_quant_v1.parquet"
OUT = DATA_ANALYTICS / "turnover_regime_quant_v1.parquet"


def main():
    print("\n=== BUILDING TURNOVER REGIME REPORT (Quant v1.0) ===\n")

    opt = pd.read_parquet(OPT)
    opt["date"] = pd.to_datetime(opt["date"])

    opt = opt.sort_values(["date", "ticker"])

    results = []
    prev_weights = None

    for d, g in opt.groupby("date"):
        g = g.sort_values("ticker")
        w = g["weight"].values
        regime = g["regime"].iloc[0]

        if prev_weights is None or len(prev_weights) != len(w):
            turnover = 0.0   # governed: no NaNs allowed
        else:
            turnover = 0.5 * np.abs(w - prev_weights).sum()

        results.append(
            {
                "date": d,
                "regime": regime,
                "turnover": float(turnover),
            }
        )

        prev_weights = w

    out = pd.DataFrame(results).sort_values("date")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)

    print(f"✔ Wrote turnover_regime_quant_v1.parquet → {OUT}\n")
    print("🎉 Turnover regime reporting built successfully.\n")


if __name__ == "__main__":
    main()