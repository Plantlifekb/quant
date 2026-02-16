#!/usr/bin/env python3
"""
Canonical weekly long-short selection builder.

Inputs:
  C:/Quant/data/analytics/quant_portfolio_weights_ensemble_risk_longshort_v2.csv
  C:/Quant/data/analytics/quant_expected_returns_timeseries.csv

Output:
  C:/Quant/data/signals/weekly_selection_longshort_canonical.csv

Schema:
  date,ticker,score,side,weight
"""

from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(r"C:\Quant")
ANALYTICS = ROOT / "data" / "analytics"
SIGNALS = ROOT / "data" / "signals"
SIGNALS.mkdir(parents=True, exist_ok=True)

WEIGHTS = ANALYTICS / "quant_portfolio_weights_ensemble_risk_longshort_v2.csv"
EXPECTED = ANALYTICS / "quant_expected_returns_timeseries.csv"
OUT = SIGNALS / "weekly_selection_longshort_canonical.csv"

print(">>> RUNNING CANONICAL BUILDER <<<")

# ------------------------------------------------------------
# Load and weekly-align weights
# ------------------------------------------------------------
def load_weights():
    df = pd.read_csv(WEIGHTS, parse_dates=["date"])
    df.columns = [c.lower().strip() for c in df.columns]
    df["ticker"] = df["ticker"].str.upper().str.strip()

    if "weight_longshort_v2" not in df.columns:
        raise ValueError("Missing weight_longshort_v2 in weights file.")

    df = df.rename(columns={"weight_longshort_v2": "weight"})

    # weekly key (Monday)
    df["week_start"] = (
        df["date"]
        .dt.to_period("W-MON")
        .apply(lambda r: r.start_time.normalize())
    )

    # collapse to weekly: last weight in the week per ticker
    df = (
        df.sort_values(["ticker", "week_start", "date"])
          .groupby(["week_start", "ticker"], as_index=False)
          .tail(1)
    )

    return df[["week_start", "ticker", "weight"]]


# ------------------------------------------------------------
# Load and collapse expected returns to weekly
# ------------------------------------------------------------
def load_expected_weekly():
    df = pd.read_csv(EXPECTED, parse_dates=["date"])
    df.columns = [c.lower().strip() for c in df.columns]
    df["ticker"] = df["ticker"].str.upper().str.strip()

    # normalise expected return column name
    if "expected_return" not in df.columns:
        for alt in ["exp_return", "pred", "expected"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "expected_return"})
                break

    if "expected_return" not in df.columns:
        raise ValueError("Missing expected_return column.")

    # weekly key
    df["week_start"] = (
        df["date"]
        .dt.to_period("W-MON")
        .apply(lambda r: r.start_time.normalize())
    )

    # collapse to one expected return per ticker per week (last value)
    df = (
        df.sort_values(["ticker", "week_start", "date"])
          .groupby(["week_start", "ticker"], as_index=False)
          .tail(1)
    )

    return df[["week_start", "ticker", "expected_return"]]


# ------------------------------------------------------------
# Build weekly long-short selection
# ------------------------------------------------------------
def build_weekly(weights_w, exp_w):
    df = weights_w.merge(exp_w, on=["week_start", "ticker"], how="left")

    # long/short side
    df["side"] = np.where(df["weight"] >= 0, "long", "short")
    df["abs_weight"] = df["weight"].abs()

    # normalise long and short books separately
    df["weight"] = df.groupby(["week_start", "side"])["abs_weight"].transform(
        lambda w: w / w.sum() if w.sum() > 0 else 0
    )

    df["score"] = df["expected_return"]

    out = df[["week_start", "ticker", "score", "side", "weight"]].copy()
    out = out.rename(columns={"week_start": "date"})

    return out.sort_values(["date", "side", "ticker"]).reset_index(drop=True)


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    w = load_weights()
    e = load_expected_weekly()
    out = build_weekly(w, e)
    out.to_csv(OUT, index=False)
    print("Wrote:", OUT)


if __name__ == "__main__":
    main()