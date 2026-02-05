# tests/integration/test_pipeline_success.py
import subprocess, os
from pathlib import Path

LOG = Path("tmp/pipeline.log")
CANONICAL = Path("data/canonical")
ARTIFACTS = [
    Path("data/analytics/quant_prices_v1.csv"),
    Path("data/analytics/strategy_returns.parquet"),
    Path("data/analytics/quant_weekly_picks_quant_v1.parquet"),
    Path("data/analytics/weekly_selection_canonical.csv")
]

def test_master_pipeline_completed_and_artifacts():
    os.makedirs("tmp", exist_ok=True)
    with open(LOG, "w", encoding="utf8") as logf:
        subprocess.check_call(
            ["python", "scripts/master_pipeline_quant_v1.py", "--snapshot", str(CANONICAL)],
            stdout=logf, stderr=subprocess.STDOUT
        )
    txt = LOG.read_text(encoding="utf8", errors="ignore")
    assert "MASTER PIPELINE COMPLETED SUCCESSFULLY" in txt
    assert any(p.exists() for p in ARTIFACTS), f"No expected artifacts found. Checked: {[str(p) for p in ARTIFACTS]}"
