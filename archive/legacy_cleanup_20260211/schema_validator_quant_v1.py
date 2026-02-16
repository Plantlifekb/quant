# ============================================================
# Module: schema_validator_quant_v1.py
# Quant Version: v1.0
# Purpose:
#   Validate governed Quant v1.0 output schemas:
#       enriched_daily.csv
#       weekly_top10_quant_v1.csv
#       weekly_backtest_quant_v1.csv
#       dashboard_top10_quant_v1.csv
#       dashboard_growth_quant_v1.csv
#
# Inputs:
#   C:\Quant\data\enriched\enriched_daily.csv
#   C:\Quant\data\ranking\weekly_top10_quant_v1.csv
#   C:\Quant\data\backtest\weekly_backtest_quant_v1.csv
#   C:\Quant\data\dashboard\dashboard_top10_quant_v1.csv
#   C:\Quant\data\dashboard\dashboard_growth_quant_v1.csv
#
# Outputs:
#   None (raises on failure)
#
# Governance Rules:
#   - No schema drift.
#   - No silent changes.
#   - Lowercase column names only.
#   - ISO‑8601 dates only.
#   - Deterministic behaviour.
#
# Logging Rules:
#   - Must integrate with logging_quant_v1.py (future).
#
# Encoding:
#   - UTF‑8 CSV inputs.
#
# Dependencies:
#   - pandas, pathlib
#
# Provenance:
#   - Part of governed Quant v1.0 validation pipeline.
#   - Any modification requires version bump.
# ============================================================

import pandas as pd
from pathlib import Path

BASE = Path(r"C:\Quant\data")

FILES_AND_SCHEMAS = {
    BASE / "enriched" / "enriched_daily.csv": [
        "date","ticker","company_name","market_sector",
        "open","high","low","close","adj_close","volume","run_date",
        "ret_1d","ret_20d","vol_20d","score",
        "mkt_avg","mkt_ma200","mkt_trend","is_monday"
    ],
    BASE / "ranking" / "weekly_top10_quant_v1.csv": [
        "date","ticker","score","rank","mkt_trend"
    ],
    BASE / "backtest" / "weekly_backtest_quant_v1.csv": [
        "date","ret_1w","cumulative_return"
    ],
    BASE / "dashboard" / "dashboard_top10_quant_v1.csv": [
        "date","ticker","company_name","market_sector",
        "score","rank","mkt_trend"
    ],
    BASE / "dashboard" / "dashboard_growth_quant_v1.csv": [
        "date","weekly_return","cumulative_return"
    ],
}

def validate_schema(path: Path, expected_cols: list) -> None:
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]

    actual = list(df.columns)
    if actual != expected_cols:
        raise ValueError(
            f"Quant v1.0 schema validation failed for {path}.\n"
            f"Expected:\n{expected_cols}\n\n"
            f"Actual:\n{actual}"
        )

def main() -> None:
    for path, schema in FILES_AND_SCHEMAS.items():
        validate_schema(path, schema)

if __name__ == "__main__":
    main()