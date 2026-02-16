import json, sys
from pathlib import Path
import importlib.util
import pandas as pd

ROOT = Path(r"C:\Quant")
ANALYSIS = ROOT / "analysis"
REALIZED = ANALYSIS / "realized_weekly_from_ingest.csv"
MODULE_PATH = Path(r"C:\Quant\scripts\regen_and_verify.py")

out = {"status":"failed","artifacts":[],"issues":[],"message":""}

if not MODULE_PATH.exists():
    out["issues"].append("regen_and_verify.py not found")
    print(json.dumps(out)); sys.exit(1)
if not REALIZED.exists():
    out["issues"].append("prebuilt realized file not found: " + str(REALIZED))
    print(json.dumps(out)); sys.exit(1)

spec = importlib.util.spec_from_file_location("regen_mod", str(MODULE_PATH))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

realized = pd.read_csv(REALIZED)
if "week" in realized.columns:
    realized["week"] = pd.to_datetime(realized["week"], errors="coerce").dt.normalize()
elif "date" in realized.columns:
    realized["week"] = pd.to_datetime(realized["date"], errors="coerce").dt.normalize()
else:
    out["issues"].append("realized file missing week/date column")
    print(json.dumps(out)); sys.exit(1)

artifacts = []
runs = []
for strat in ("longonly","longshort"):
    pick_path = mod.find_first_existing(mod.PICK_CANDIDATES[strat])
    if not pick_path:
        runs.append({"strategy":strat,"status":"no_picks"})
        out["issues"].append(f"No pick file found for {strat}")
        continue
    picks = mod.load_picks(pick_path)
    if picks is None or picks.empty:
        runs.append({"strategy":strat,"status":"no_picks_or_empty"})
        out["issues"].append(f"Picks empty for {strat} at {pick_path}")
        continue
    picks_matched = mod.safe_match(picks, realized)
    res = mod.compute_and_save(strat, picks_matched)
    runs.append(res)
    artifacts.extend([mod.OUT["regen_"+strat], mod.OUT["summary_"+strat], mod.OUT["diag_"+strat], mod.OUT["png_"+strat]])

out["status"] = "success"
out["artifacts"] = [str(a) for a in artifacts]
out["runs"] = runs
out["issues"] = out["issues"]
out["message"] = "verification run complete (used prebuilt realized file)"
print(json.dumps(out))
