import pandas as pd
from pathlib import Path


def main() -> None:
    root = Path("C:/Quant")

    # INPUT: your new v2 dashboard inputs
    dashboard_inputs_path = (
        root / "data" / "analytics" / "reporting" / "quant_dashboard_inputs_v2.csv"
    )

    # OUTPUT: write next to the v1 file, same folder
    dashboard_out_path = (
        root / "data" / "analytics" / "reporting" / "dashboard_returns_v2.csv"
    )

    df = pd.read_csv(dashboard_inputs_path)

    # For now, dashboard = summary table
    dashboard = df.copy()

    dashboard.to_csv(dashboard_out_path, index=False)

    print("Dashboard v2 generated.")
    print(f"Output: {dashboard_out_path}")


if __name__ == "__main__":
    main()