from pathlib import Path
def load_tickers(filename="ticker_reference.csv"):
    p = Path(__file__).resolve().parent.parent / filename
    if not p.exists():
        return []
    with p.open(encoding="utf8") as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
