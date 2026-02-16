"""
Quant v2.0 — Alpha Engine (Sector-Neutral, 4-Pillar Blended Alpha)

Builds a stable, interpretable alpha signal using:

    • Momentum (21d + 63d)
    • Volatility (defensive low-vol tilt)
    • Liquidity (volume + spread)
    • Size (small-cap tilt)

All signals:
    • winsorised
    • z-scored cross-sectionally
    • sector-neutralised
    • blended into alpha_raw and alpha_z

Output:
    C:\Quant\data\signals\alpha_quant_v2.parquet
"""

from pathlib import Path
import numpy as np
import pandas as pd

BASE = Path(r"C:\Quant")

FUNDAMENTALS = BASE / "data" / "ingestion" / "fundamentals.parquet"
ALPHA_OUT = BASE / "data" / "signals" / "alpha_quant_v2.parquet"


def winsorize(s, lower=0.01, upper=0.99):
    lo = s.quantile(lower)
    hi = s.quantile(upper)
    return s.clip(lo, hi)


def zscore(s):
    mu = s.mean()
    sd = s.std(ddof=0)
    if sd == 0 or np.isnan(sd):
        return pd.Series(0.0, index=s.index)
    return (s - mu) / sd


def sector_neutral(df, col, sector_col="sector_clean"):
    return df[col] - df.groupby(sector_col)[col].transform("mean")


def main():
    print("\n=== BUILDING ALPHA (Quant v2.0) ===\n")

    if not FUNDAMENTALS.exists():
        raise FileNotFoundError(f"Missing fundamentals file: {FUNDAMENTALS}")

    df = pd.read_parquet(FUNDAMENTALS)
    df["date"] = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str).str.upper()

    # Clean sector
    df["sector_clean"] = (
        df["sector"]
        .fillna("UNKNOWN_SECTOR")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # Sort for rolling ops
    df = df.sort_values(["ticker", "date"]).copy()

    # === MOMENTUM ===
    df["log_ret"] = df["log_ret"].astype(float)
    df["mom_21"] = df.groupby("ticker")["log_ret"].rolling(21).sum().reset_index(level=0, drop=True)
    df["mom_63"] = df.groupby("ticker")["log_ret"].rolling(63).sum().reset_index(level=0, drop=True)
    df["MOM"] = 0.5 * df["mom_21"] + 0.5 * df["mom_63"]

    # === VOLATILITY (low-vol tilt) ===
    for col in ["vol_5", "vol_20", "high_low_spread"]:
        df[col] = df[col].astype(float)

    vol_raw = (
        0.5 * df["vol_20"].fillna(df["vol_20"].median())
        + 0.3 * df["vol_5"].fillna(df["vol_5"].median())
        + 0.2 * df["high_low_spread"].fillna(df["high_low_spread"].median())
    )
    df["VOL"] = -vol_raw  # invert: lower vol → higher alpha

    # === LIQUIDITY ===
    df["volume"] = df["volume"].astype(float)
    df["volume_z"] = zscore(df["volume"].replace(0, np.nan).fillna(df["volume"].median()))
    df["spread_z"] = zscore(df["high_low_spread"].fillna(df["high_low_spread"].median()))
    df["LIQ"] = 0.6 * df["volume_z"] - 0.4 * df["spread_z"]

    # === SIZE (small-cap tilt) ===
    df["close"] = df["close"].astype(float)
    size_proxy = (df["volume"].clip(lower=1.0) * df["close"].clip(lower=0.01)).apply(np.log)
    df["SIZE"] = -size_proxy  # invert: smaller → higher alpha

    # === NORMALISE + SECTOR-NEUTRALISE PER DATE ===
    alpha_rows = []

    for date, g in df.groupby("date"):
        g = g.copy()

        for col in ["MOM", "VOL", "LIQ", "SIZE"]:
            s = winsorize(g[col].astype(float))
            g[col] = zscore(s)
            g[col] = sector_neutral(g, col)

        # Blend
        g["alpha_raw"] = (
            0.35 * g["MOM"]
            + 0.25 * g["VOL"]
            + 0.25 * g["LIQ"]
            + 0.15 * g["SIZE"]
        )

        # Final z-score
        g["alpha_z"] = zscore(g["alpha_raw"])

        alpha_rows.append(g[["date", "ticker", "alpha_raw", "alpha_z"]])

    alpha = pd.concat(alpha_rows, axis=0).sort_values(["date", "ticker"]).reset_index(drop=True)

    # Write output
    ALPHA_OUT.parent.mkdir(parents=True, exist_ok=True)
    alpha.to_parquet(ALPHA_OUT, index=False)

    print(f"✔ Wrote alpha to: {ALPHA_OUT}")
    print("\n🎉 Alpha Engine (Quant v2.0) completed successfully.\n")


if __name__ == "__main__":
    main()