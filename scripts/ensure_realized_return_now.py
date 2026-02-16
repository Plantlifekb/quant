import pandas as pd
from pathlib import Path
p = Path(r"C:\Quant\analysis\realized_weekly_from_ingest.csv")
if not p.exists():
    print("MISSING_REALIZED_FILE"); raise SystemExit(1)
df = pd.read_csv(p)
if "realized_return" not in df.columns:
    if "close_end" in df.columns and "close_start" in df.columns:
        df["realized_return"] = df["close_end"] / df["close_start"] - 1.0
        df.to_csv(p, index=False)
        print("CREATED_REALIZED_RETURN_FROM_CLOSES")
    else:
        print("NO_SOURCE_FOR_REALIZED_RETURN", df.columns.tolist())
        raise SystemExit(2)
else:
    print("REALIZED_RETURN_PRESENT")
