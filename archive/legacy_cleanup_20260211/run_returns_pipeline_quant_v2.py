import pandas as pd
from pathlib import Path

# Correct imports based on your real project structure
from scripts.analytics.return_engine import return_engine
from scripts.reporting.reporting_engine_returns_v2 import write_dashboard_inputs_v2


def main() -> None:
    """
    Run the governed v2 return engine pipeline:
        1. Load raw prices
        2. Compute full return suite
        3. Write enriched returns timeseries (v2)
        4. Write dashboard inputs (v2)
    """

    root = Path("C:/Quant")

    # --- INPUTS ---
    prices_path = root / "data" / "analytics" / "quant_prices_v1.csv"

    # --- OUTPUTS ---
    returns_out_path = root / "data" / "analytics" / "quant_returns_timeseries_v2.csv"
    dashboard_out_path = (
        root / "data" / "analytics" / "reporting" / "quant_dashboard_inputs_v2.csv"
    )

    # --- LOAD PRICES ---
    prices = pd.read_csv(prices_path)

    # ensure date column is parsed
    if "date" in prices.columns:
        prices["date"] = pd.to_datetime(prices["date"])

    # --- RUN RETURN ENGINE ---
    prices_enriched = return_engine(prices)

    # --- WRITE OUTPUTS ---
    prices_enriched.to_csv(returns_out_path, index=False)

    write_dashboard_inputs_v2(
        prices_enriched,
        str(dashboard_out_path)
    )

    print("Return engine v2 pipeline completed successfully.")
    print(f"Returns written to: {returns_out_path}")
    print(f"Dashboard inputs written to: {dashboard_out_path}")


if __name__ == "__main__":
    main()