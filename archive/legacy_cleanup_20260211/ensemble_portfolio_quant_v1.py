# --- CHANGES IN THIS PATCH ---
# 1. When loading composite_v1, also load:
#       market_sector, low_vol_252, volume_zscore
# 2. Carry these fields through the merge
# 3. Include them in the final output
# -----------------------------------------------

def load_composite() -> pd.DataFrame:
    logger.info(f"Loading base composite factors from {COMPOSITE_FILE}")
    df = pd.read_csv(COMPOSITE_FILE)

    required = {
        "date", "ticker", "ret", "composite_signal_v1",
        "market_sector", "low_vol_252", "volume_zscore"
    }
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing required columns in composite file: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=[
        "date", "ticker", "ret", "composite_signal_v1",
        "market_sector", "low_vol_252", "volume_zscore"
    ])

    df["ret"] = pd.to_numeric(df["ret"], errors="coerce")
    df["composite_signal_v1"] = pd.to_numeric(df["composite_signal_v1"], errors="coerce")
    df["low_vol_252"] = pd.to_numeric(df["low_vol_252"], errors="coerce")
    df["volume_zscore"] = pd.to_numeric(df["volume_zscore"], errors="coerce")

    df = df.dropna(subset=[
        "ret", "composite_signal_v1", "low_vol_252", "volume_zscore"
    ])

    logger.info(f"Loaded {len(df)} rows from composite file after cleaning.")
    return df[
        [
            "date", "ticker", "ret",
            "composite_signal_v1",
            "market_sector", "low_vol_252", "volume_zscore"
        ]
    ]


def build_ensemble(base, mh, regimes):
    logger.info("Building ensemble signal with risk-model fields preserved.")

    merged = base.merge(
        mh[["date", "ticker", "composite_mh_signal"]],
        on=["date", "ticker"],
        how="inner",
    )

    merged = merged.merge(regimes, on="date", how="left")
    merged["regime_score"] = merged["regime_score"].fillna(0.5)

    span = MAX_REGIME_MULTIPLIER - MIN_REGIME_MULTIPLIER
    merged["regime_multiplier"] = MIN_REGIME_MULTIPLIER + span * merged["regime_score"]

    def _per_date(group):
        g = group.copy()
        g["composite_v1_z"] = _zscore(g["composite_signal_v1"])
        g["composite_mh_z"] = _zscore(g["composite_mh_signal"])
        g["ensemble_raw"] = (
            W_COMPOSITE_V1 * g["composite_v1_z"] +
            W_COMPOSITE_MH * g["composite_mh_z"]
        )
        g["ensemble_signal_v1"] = g["ensemble_raw"] * g["regime_multiplier"]
        return g

    out = (
        merged.groupby("date", group_keys=False)
        .apply(_per_date)
        .reset_index(drop=True)
    )

    return out[
        [
            "date", "ticker", "ret",
            "composite_signal_v1",
            "composite_mh_signal",
            "regime_score",
            "market_sector", "low_vol_252", "volume_zscore",
            "composite_v1_z", "composite_mh_z",
            "ensemble_signal_v1",
        ]
    ]