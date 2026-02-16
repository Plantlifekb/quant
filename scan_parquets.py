#!/usr/bin/env python3
import json
from pathlib import Path
import pandas as pd

ANALYTICS = Path(r"C:\Quant\data\analytics")
OUT_JSON = ANALYTICS / "parquet_inventory.json"
SAMPLE_ROWS = 5

def inspect_parquet(p: Path):
    info = {"file": str(p.name), "path": str(p), "size_bytes": p.stat().st_size}
    try:
        df = pd.read_parquet(p, engine="pyarrow")
        info["rows"] = int(len(df))
        info["columns"] = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            info["columns"].append({"name": col, "dtype": dtype})
        info["sample"] = df.head(SAMPLE_ROWS).to_dict(orient="records")
    except Exception as e:
        info["error"] = repr(e)
    return info

def main():
    inventory = []
    if not ANALYTICS.exists():
        print("Analytics path not found:", ANALYTICS)
        return
    for p in sorted(ANALYTICS.glob("*.parquet"), key=lambda x: x.stat().st_size, reverse=True):
        print("Inspecting", p.name)
        inventory.append(inspect_parquet(p))
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2, default=str)
    print("Wrote", OUT_JSON)
    for item in inventory:
        if "error" in item:
            print(f"{item['file']}: ERROR {item['error']}")
        else:
            cols = ", ".join([c["name"] for c in item["columns"][:8]])
            print(f"{item['file']}: rows={item['rows']} cols={len(item['columns'])} sample_cols={cols}")

if __name__ == "__main__":
    main()