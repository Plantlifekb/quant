# tests/integration/test_end_to_end.py
import subprocess, os
from pathlib import Path

LOG = Path("tmp/pipeline.log")
CANONICAL = Path("data/canonical")

def test_end_to_end_pipeline_runs():
    os.makedirs("tmp", exist_ok=True)
    with open(LOG, "w", encoding="utf8") as logf:
        subprocess.check_call(
            ["python", "scripts/master_pipeline_quant_v1.py", "--snapshot", str(CANONICAL)],
            stdout=logf, stderr=subprocess.STDOUT
        )
    assert "MASTER PIPELINE COMPLETED SUCCESSFULLY" in LOG.read_text(encoding="utf8", errors="ignore")