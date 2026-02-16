"""
attribution_diagnostics_v1.py
Quant v1.0 — Attribution Suite
--------------------------------

Purpose:
    Validate the integrity of all attribution-suite inputs and outputs.
    Detects:
        - missing factor returns
        - exposure drift
        - turnover spikes
        - liquidity anomalies
        - regime inconsistencies
        - timestamp misalignment
        - missing or corrupted files

Outputs:
    C:\Quant\data\analytics\attribution_outputs_v1\attribution_diagnostics.csv
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from logging_attribution_suite_v1 import get_logger

logger = get_logger("attribution_diagnostics_v1")

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Inputs
EXPOSURES_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factor_exposures_timeseries.csv"
ALIGNED_RETURNS_FILE = PROJECT_ROOT / "data" / "analytics" / "attribution_outputs_v1" / "factor_returns_aligned.csv"
ATTR_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_attribution_regime_v1.csv"
TURNOVER_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_turnover_timeseries.csv"
LIQ_FILE = PROJECT_ROOT / "data" / "analytics" / "attribution_outputs_v1" / "liquidity_costs_enhanced.csv"
REGIME_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_liquidity_regime_timeseries.csv"

# Output
OUT_DIR = PROJECT_ROOT / "data" / "analytics" / "attribution_outputs_v1"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "attribution_diagnostics.csv"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------
def safe_load(path: Path, name: str) -> pd.DataFrame:
    if not path.exists():
        logger.error("Missing required file: %s", path)
        return pd.DataFrame({"error": [f"Missing file: {name}"]})

    try:
        df = pd.read_csv(path)
        df.columns = [c.lower() for c in df.columns]
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], utc=True)
        return df
    except Exception as e:
        logger.error("Failed to load %s: %s", name, str(e))
        return pd.DataFrame({"error": [f"Failed to load {name}: {e}"]})


# ---------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------
def check_missing_factor_returns(exposures, aligned_returns):
    if "error" in exposures.columns or "error" in aligned_returns.columns:
        return "SKIPPED"

    exp_dates = set(exposures["date"].unique())
    ret_dates = set(aligned_returns["date"].unique())

    missing = exp_dates - ret_dates
    return len(missing)


def check_exposure_drift(exposures):
    if "error" in exposures.columns:
        return "SKIPPED"

    # Exposure drift = unusually large changes in exposures
    exposures_sorted = exposures.sort_values(["ticker", "date"])
    exposures_sorted["exposure_change"] = exposures_sorted.groupby("ticker")["exposure"].diff()

    drift_events = exposures_sorted[abs(exposures_sorted["exposure_change"]) > 0.15]
    return len(drift_events)


def check_turnover_spikes(turnover):
    if "error" in turnover.columns:
        return "SKIPPED"

    spikes = turnover[abs(turnover["turnover"]) > 0.25]
    return len(spikes)


def check_liquidity_anomalies(liq):
    if "error" in liq.columns:
        return "SKIPPED"

    anomalies = liq[liq["liquidity_cost_enhanced"] > liq["liquidity_cost_enhanced"].quantile(0.995)]
    return len(anomalies)


def check_regime_inconsistencies(regime):
    if "error" in regime.columns:
        return "SKIPPED"

    # Regime should not flip more than 3 times in 5 days
    regime_sorted = regime.sort_values("date")
    regime_sorted["shift"] = regime_sorted["liquidity_regime"] != regime_sorted["liquidity_regime"].shift(1)
    regime_sorted["block"] = regime_sorted["shift"].cumsum()

    block_sizes = regime_sorted.groupby("block")["date"].count()
    rapid_flips = (block_sizes < 2).sum()

    return rapid_flips


def check_timestamp_alignment(exposures, aligned_returns):
    if "error" in exposures.columns or "error" in aligned_returns.columns:
        return "SKIPPED"

    exp_dates = exposures["date"].unique()
    ret_dates = aligned_returns["date"].unique()

    return len(set(exp_dates) ^ set(ret_dates))


# ---------------------------------------------------------------------
# Main diagnostics runner
# ---------------------------------------------------------------------
def run_diagnostics():
    logger.info("Running attribution diagnostics.")

    exposures = safe_load(EXPOSURES_FILE, "exposures")
    aligned_returns = safe_load(ALIGNED_RETURNS_FILE, "aligned factor returns")
    turnover = safe_load(TURNOVER_FILE, "turnover")
    liq = safe_load(LIQ_FILE, "enhanced liquidity costs")
    regime = safe_load(REGIME_FILE, "liquidity regime")

    diagnostics = {
        "diagnostics_run_at": iso_now(),
        "missing_factor_return_dates": check_missing_factor_returns(exposures, aligned_returns),
        "exposure_drift_events": check_exposure_drift(exposures),
        "turnover_spike_events": check_turnover_spikes(turnover),
        "liquidity_anomalies": check_liquidity_anomalies(liq),
        "regime_inconsistencies": check_regime_inconsistencies(regime),
        "timestamp_misalignment": check_timestamp_alignment(exposures, aligned_returns),
    }

    return pd.DataFrame([diagnostics])


# ---------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------
def save_output(df: pd.DataFrame):
    logger.info("Saving diagnostics report to %s", OUT_FILE)
    df.to_csv(OUT_FILE, index=False, encoding="utf-8")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    logger.info("Starting attribution_diagnostics_v1 run.")

    df = run_diagnostics()
    save_output(df)

    logger.info("attribution_diagnostics_v1 completed successfully.")


if __name__ == "__main__":
    main()