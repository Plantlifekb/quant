# tests/unit/test_master_pipeline.py
import subprocess, os
from pathlib import Path

LOG = Path("tmp/pipeline.log")
CANONICAL = Path("data/canonical")

def test_master_pipeline_smoke_runs_and_logs():
    os.makedirs("tmp", exist_ok=True)
    with open(LOG, "w", encoding="utf8") as logf:
        subprocess.check_call(
            ["python", "scripts/master_pipeline_quant_v1.py", "--snapshot", str(CANONICAL)],
            stdout=logf, stderr=subprocess.STDOUT
        )
    txt = LOG.read_text(encoding="utf8", errors="ignore")
    assert "MASTER PIPELINE COMPLETED SUCCESSFULLY" in txt
