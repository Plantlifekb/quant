# C:\Quant\scripts\maintenance\cleanup_quant.py
import shutil, time
from pathlib import Path
ROOT = Path("C:/Quant")
ARCH = ROOT / "archive"
ARCH.mkdir(exist_ok=True)
now = time.time()
# Move old fixed files older than 30 days to archive
for p in (ROOT/"data"/"analytics").glob("strategy_returns_*.parquet"):
    if now - p.stat().st_mtime > 30*24*3600:
        shutil.move(str(p), str(ARCH/p.name))
print("Cleanup complete")