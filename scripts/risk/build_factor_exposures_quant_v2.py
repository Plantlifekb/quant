"""
Quant v2.0 — Factor Exposure Engine

Builds a real factor exposure matrix with:

    • Sector factors (1-hot)
    • Industry factors (1-hot)
    • Style factors (raw + sector-neutralised):
        - Momentum (MOM_raw, MOM_neutral)
        - Volatility (VOL_raw, VOL_neutral)
        - Liquidity (LIQ_raw, LIQ_neutral)
        - Size (SIZE_raw, SIZE_neutral)      [proxy]
        - Value (VALUE_raw, VALUE_neutral)  [scaffolded]

Input:
    C:\Quant\data\ingestion\fundamentals.parquet

Output:
    C:\Quant\data\risk\factor_exposures_quant_v2.parquet
"""

from pathlib import Path
import numpy as np
import pandas as pd

BASE = Path(r"C:\Quant")
FUNDAMENTALS = BASE / "data" / "ingestion" / "fundamentals.parquet"
OUT = BASE / "data" / "risk" / "factor_exposures_quant_v2.parquet"


def winsorize_series(s: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    lo = s.quantile(lower)
    hi = s.quantile(upper)
    return s.clip(lo, hi)


def zscore_series(s: pd.Series) -> pd.Series:
    mu = s.mean()
    sigma = s.std(ddof=0)
    if sigma == 0 or np.isnan(sigma):
        return pd.Series(0.0, index=s.index)
    return (s - mu) / sigma


def sector_neutralise(df: pd.DataFrame, factor_col: str, sector_col: str) -> pd.Series:
    """
    Simple sector-neutralisation: subtract sector mean from each observation.
    """
    return df[factor_col] - df.groupby(sector_col)[factor_col].transform("mean")


def build_sector_industry_dummies(df: pd.DataFrame) -> pd.DataFrame:
    df["sector_clean"] = (
        df["sector"].fillna("UNKNOWN_SECTOR").astype(str).str.strip().str.upper()
    )
    df["industry_clean"] = (
        df["industry"].fillna("UNKNOWN_INDUSTRY").astype(str).str.strip().str.upper()
    )

    sector_dummies = pd.get_dummies(df["sector_clean"], prefix="SEC", dtype=float)
    industry_dummies = pd.get_dummies(df["industry_clean"], prefix="IND", dtype=float)

    return pd.concat([df[["date", "ticker"]], sector_dummies, industry_dummies], axis=1)


def build_style_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build raw + sector-neutralised style factors per date cross-section.
    Assumes full history is available.
    """
    out_rows = []

    # Sort for rolling operations
    df = df.sort_values(["ticker", "date"]).copy()

    # Momentum: rolling 21d and 63d log returns
    df["log_ret"] = df["log_ret"].astype(float)
    df["mom_21"] = df.groupby("ticker")["log_ret"].rolling(21).sum().reset_index(level=0, drop=True)
    df["mom_63"] = df.groupby("ticker")["log_ret"].rolling(63).sum().reset_index(level=0, drop=True)
    df["MOM_raw"] = 0.5 * df["mom_21"] + 0.5 * df["mom_63"]

    # Volatility: combine vol_5, vol_20, high_low_spread
    for col in ["vol_5", "vol_20", "high_low_spread"]:
        df[col] = df[col].astype(float)
    df["VOL_raw"] = (
        0.5 * df["vol_20"].fillna(df["vol_20"].median())
        + 0.3 * df["vol_5"].fillna(df["vol_5"].median())
        + 0.2 * df["high_low_spread"].fillna(df["high_low_spread"].median())
    )

    # Liquidity: volume, volume_zscore, inverse spread
    df["volume"] = df["volume"].astype(float)
    df["volume_zscore"] = df["volume_zscore"].astype(float)
    df["LIQ_raw"] = (
        0.5 * zscore_series(df["volume"].replace(0, np.nan).fillna(df["volume"].median()))
        + 0.3 * df["volume_zscore"].fillna(0.0)
        - 0.2 * zscore_series(df["high_low_spread"].fillna(df["high_low_spread"].median()))
    )

    # Size (proxy): log(volume * close)
    df["close"] = df["close"].astype(float)
    size_proxy = (df["volume"].clip(lower=1.0) * df["close"].clip(lower=0.01)).apply(np.log)
    df["SIZE_raw"] = size_proxy

    # Value: scaffolded zeros
    df["VALUE_raw"] = 0.0

    # Now normalise and sector-neutralise per date cross-section
    style_cols = ["MOM_raw", "VOL_raw", "LIQ_raw", "SIZE_raw", "VALUE_raw"]

    for date, g in df.groupby("date"):
        g = g.copy()
        # Winsorise + z-score raw factors
        for col in style_cols:
            s = g[col].astype(float)
            s = winsorize_series(s)
            g[col] = zscore_series(s)

        # Sector-neutralised versions
        for col in style_cols:
            neutral = sector_neutralise(g, col, "sector_clean")
            neutral = zscore_series(neutral)
            g[col.replace("_raw", "_neutral")] = neutral

        out_rows.append(
            g[
                ["date", "ticker"]
                + style_cols
                + [c.replace("_raw", "_neutral") for c in style_cols]
            ]
        )

    styles = pd.concat(out_rows, axis=0).sort_values(["date", "ticker"]).reset_index(drop=True)
    return styles


def main():
    print("\n=== BUILDING FACTOR EXPOSURES (Quant v2.0) ===\n")

    if not FUNDAMENTALS.exists():
        raise FileNotFoundError(f"Missing fundamentals file: {FUNDAMENTALS}")

    df = pd.read_parquet(FUNDAMENTALS)
    df["date"] = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str).str.upper()

    # Ensure sector/industry exist
    if "sector" not in df.columns or "industry" not in df.columns:
        raise ValueError("fundamentals.parquet must contain 'sector' and 'industry' columns")

    # Clean sector/industry for later use
    df["sector"] = df["sector"].fillna("UNKNOWN_SECTOR")
    df["industry"] = df["industry"].fillna("UNKNOWN_INDUSTRY")

    # 1) Sector + industry dummies
    print("• Building sector + industry factors ...")
    sec_ind = build_sector_industry_dummies(df)

    # 2) Style factors (raw + sector-neutral)
    print("• Building style factors (raw + sector-neutral) ...")
    styles = build_style_factors(df)

    # 3) Merge all exposures
    print("• Merging exposures ...")
    exposures = (
        sec_ind.merge(styles, on=["date", "ticker"], how="inner")
        .sort_values(["date", "ticker"])
        .reset_index(drop=True)
    )

    # 4) Write output
    OUT.parent.mkdir(parents=True, exist_ok=True)
    exposures.to_parquet(OUT, index=False)
    print(f"\n✔ Wrote factor exposures to: {OUT}")
    print("\n🎉 Factor Exposure Engine completed successfully (Quant v2.0).\n")


if __name__ == "__main__":
    main()