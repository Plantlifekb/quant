# ============================================
# C:\Quant\scripts\analytics\build_portfolio_quant_v2_from_positions.py
# ============================================

"""
Rebuilds portfolio_quant_v2.parquet from 5-year positions.

Inputs:
    C:\Quant\data\analytics\quant_positions_timeseries.csv

Output:
    C:\Quant\data\analytics\portfolio_quant_v2.parquet
"""

from pathlib import Path
import pandas as pd

BASE = Path(r"C:\Quant")

POS = BASE / "data" / "analytics" / "quant_positions_timeseries.csv"
PORT_OUT = BASE / "data" / "analytics" / "portfolio_quant_v2.parquet"


def main():
    print("\n=== REBUILDING PORTFOLIO (Quant v2.0) FROM POSITIONS ===\n")

    pos = pd.read_csv(POS)

    # Normalise
    pos["date"] = pd.to_datetime(pos["date"], utc=True).dt.tz_convert(None)
    pos["ticker"] = pos["ticker"].astype(str).str.upper()

    if "weight" not in pos.columns:
        raise ValueError("Expected 'weight' column not found in positions file.")

    if "strategy" not in pos.columns:
        pos["strategy"] = "CORE"

    port = pos[["date", "ticker", "weight", "strategy"]].copy()
    port = port.sort_values(["date", "ticker"]).reset_index(drop=True)

    PORT_OUT.parent.mkdir(parents=True, exist_ok=True)
    port.to_parquet(PORT_OUT, index=False)

    print(f"✔ Wrote portfolio to: {PORT_OUT}")
    print("\n🎉 Portfolio rebuild completed successfully.\n")


if __name__ == "__main__":
    main()