#!/usr/bin/env python3
"""
Canonical weekly long-only selection builder.
Inputs:
  C:/Quant/data/analytics/quant_portfolio_weights_longonly.csv
  C:/Quant/data/analytics/quant_expected_returns_timeseries.csv

Output:
  C:/Quant/data/signals/weekly_selection_canonical.csv

Schema:
  date,ticker,score,side,weight
"""

from pathlib import Path
import pandas as pd

ROOT = Path(r"C:\Quant")
ANALYTICS = ROOT / "data" / "analytics"
SIGNALS = ROOT / "data" / "signals"
SIGNALS.mkdir(parents=True, exist_ok=True)

WEIGHTS = ANALYTICS / "quant_portfolio_weights_longonly.csv"
EXPECTED = ANALYTICS / "quant_expected_returns_timeseries.csv"
OUT = SIGNALS / "weekly_selection_canonical.csv"


def load_weights():
    df = pd.read_csv(WEIGHTS, parse_dates=["date"])
    df.columns = [c.lower().strip() for c in df.columns]
    df["ticker"] = df["ticker"].str.upper().str.strip()

    if "weight_longonly" not in df.columns:
        raise ValueError("Missing column 'weight_longonly' in long-only weights file.")

    df = df.rename(columns={"weight_longonly": "weight"})
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

    df = df[df["weight"] > 0].copy()

    df["week_start"] = (
        df["date"]
        .dt.to_period("W-MON")
        .apply(lambda r: r.start_time.normalize())
    )

    df["weight"] = df.groupby("week_start")["weight"].transform(
        lambda w: w / w.sum() if w.sum() > 0 else 0
    )

    df["score"] = df["expected_return"]

    out = df[["week_start", "ticker", "score", "weight"]].copy()
    out = out.rename(columns={"week_start": "date"})
    out["side"] = "long"

    return out.sort_values(["date", "ticker"]).reset_index(drop=True)


def main():
    w = load_weights()
    e = load_expected()
    out = build_weekly(w, e)
    out.to_csv(OUT, index=False)
    print("Wrote:", OUT)


if __name__ == "__main__":
    main()