# tests/conftest.py
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]   # C:\Quant
DASHBOARD_DIR = ROOT / "scripts" / "dashboard"

root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

dash_str = str(DASHBOARD_DIR)
if dash_str not in sys.path:
    sys.path.insert(0, dash_str)