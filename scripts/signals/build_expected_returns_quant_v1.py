"""
build_expected_returns_quant_v1.py

Produces the governed Quant v1.0 signal layer:

    data/signals/expected_returns_quant_v1.parquet

Inputs:
    - prices.parquet
    - fundamentals.parquet
    - risk_model.parquet
    - securities_master.parquet
    - quant_regime_states_v1.csv

Output schema:
    date
    ticker
    expected_return
    regime
    signal_version
"""

import sys
from pathlib import Path
import pandas as pd

BASE = Path(r"C:\Quant")

# ----------------------------------------------------------------------
# Governed ingestion inputs
# ----------------------------------------------------------------------
PRICES = BASE / "data" / "ingestion" / "prices.parquet"
FUND = BASE / "data" / "ingestion" / "fundamentals.parquet"
RISK = BASE / "data" / "ingestion" / "risk_model.parquet"
MASTER = BASE / "data" / "reference" / "securities_master.parquet"

# Regime source
REGIME = BASE / "data" / "analytics" / "quant_regime_states_v1.csv"

# Output
OUT = BASE / "data" / "signals" / "expected_returns_quant_v1.parquet"


def fail(msg: str):
    print(f"\n❌ FAIL: {msg}\n")
    sys.exit(1)


def ok(msg: str):
    print(f"✔ {msg}")


def main():
    print("\n=== BUILDING EXPECTED RETURNS (Quant v1.0) ===\n")

    # ------------------------------------------------------------------
    # 1. Load governed ingestion inputs
    # ------------------------------------------------------------------
    try:
        prices = pd.read_parquet(PRICES)
        fund = pd.read_parquet(FUND)
        risk = pd.read_parquet(RISK)
        master = pd.read_parquet(MASTER)
    except Exception as e:
        fail(f"Error loading ingestion inputs: {e}")

    # ------------------------------------------------------------------
    # 2. Compute expected returns
    # ------------------------------------------------------------------
    prices["date"] = pd.to_datetime(prices["date"], utc=False)
    prices = prices.sort_values(["ticker", "date"])

    prices["expected_return"] = (
        prices.groupby("ticker")["return"]
        .rolling(window=21, min_periods=5)
        .mean()
        .reset_index(level=0, drop=True)
    )

    prices = prices.dropna(subset=["expected_return"])

    # ------------------------------------------------------------------
    # 3. Load regime states
    # ------------------------------------------------------------------
    if not REGIME.exists():
        print("⚠ No regime file found — using neutral regime.")
        regime = None
    else:
        try:
            regime = pd.read_csv(REGIME)
        except Exception as e:
            fail(f"Error reading regime file: {e}")

        regime.columns = [c.lower() for c in regime.columns]

        if "date" not in regime.columns:
            fail("Regime file must contain a 'date' column")

        # Identify regime label column
        regime_col = None
        for cand in ["regime", "regime_label", "regime_state", "market_regime"]:
            if cand in regime.columns:
                regime_col = cand
                break

        if regime_col is None:
            fail("Regime file must contain a regime label column (e.g. 'regime_label')")

        # Normalize timezone → remove UTC
        regime["date"] = pd.to_datetime(regime["date"], utc=True).dt.tz_convert(None)

        regime = regime[["date", regime_col]].rename(columns={regime_col: "regime"})
        regime = regime.drop_duplicates()

    # ------------------------------------------------------------------
    # 4. Merge expected returns with regime
    # ------------------------------------------------------------------
    signals = prices[["date", "ticker", "expected_return"]].copy()

    if regime is None:
        signals["regime"] = "neutral"
    else:
        signals = pd.merge(signals, regime, on="date", how="left")
        signals["regime"] = signals["regime"].fillna("neutral")

    # ------------------------------------------------------------------
    # 5. Enforce universe completeness
    # ------------------------------------------------------------------
    universe = set(master["ticker"].unique())

    def check_universe(group: pd.DataFrame):
        missing = universe - set(group["ticker"])
        if missing:
            fail(
                f"Missing tickers for date {group['date'].iloc[0].date()}: "
                f"{sorted(list(missing))[:10]}..."
            )
        return group

    signals.groupby("date").apply(check_universe)

    # ------------------------------------------------------------------
    # 6. Stamp metadata
    # ------------------------------------------------------------------
    signals["signal_version"] = "v1.0"

    # ------------------------------------------------------------------
    # 7. Write governed output
    # ------------------------------------------------------------------
    OUT.parent.mkdir(parents=True, exist_ok=True)
    try:
        signals.to_parquet(OUT, index=False)
    except Exception as e:
        fail(f"Error writing expected_returns_quant_v1.parquet: {e}")

    ok(f"Wrote expected_returns_quant_v1.parquet → {OUT}")
    print("\n🎉 Governed signal layer built successfully.\n")


if __name__ == "__main__":
    main()