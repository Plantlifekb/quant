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


def load_weights():
    df = pd.read_csv(WEIGHTS, parse_dates=["date"])
    df.columns = [c.lower().strip() for c in df.columns]
    df["ticker"] = df["ticker"].str.upper().str.strip()

    if "weight_longshort_v2" not in df.columns:
        raise ValueError("Missing 'weight_longshort_v2' column in long-short weights file.")

    df = df.rename(columns={"weight_longshort_v2": "weight"})
    return df[["date", "ticker", "weight"]]


def load_expected():
    df = pd.read_csv(EXPECTED, parse_dates=["date"])
    df.columns = [c.lower().strip() for c in df.columns]
    df["ticker"] = df["ticker"].str.upper().str.strip()

    if "expected_return" not in df.columns:
        for alt in ["exp_return", "pred", "expected"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "expected_return"})
                break

    if "expected_return" not in df.columns:
        raise ValueError("Missing expected return column.")

    return df[["date", "ticker", "expected_return"]]


def build_weekly(weights, exp):
    df = weights.merge(exp, on=["date", "ticker"], how="left")

    df["week_start"] = (
        df["date"]
        .dt.to_period("W-MON")
        .apply(lambda r: r.start_time.normalize())
    )

    df["side"] = np.where(df["weight"] >= 0, "long", "short")
    df["abs_weight"] = df["weight"].abs()

    df["weight"] = df.groupby(["week_start", "side"])["abs_weight"].transform(
        lambda w: w / w.sum() if w.sum() > 0 else 0
    )

    df["score"] = df["expected_return"]

    out = df[["week_start", "ticker", "score", "side", "weight"]].copy()
    out = out.rename(columns={"week_start": "date"})

    return out.sort_values(["date", "side", "ticker"]).reset_index(drop=True)


def main():
    w = load_weights()
    e = load_expected()
    out = build_weekly(w, e)
    out.to_csv(OUT, index=False)
    print("Wrote:", OUT)


if __name__ == "__main__":
    main()