"""
verification_quant_pipeline.py
Full import verification for the Quant orchestrator.
Does NOT run the real pipeline tasks.
"""

import os
import sys
import importlib

print("=== Quant Pipeline Verification ===")

# --- 1. Verify PYTHONPATH ---
expected = r"C:\Quant\src"
actual = os.environ.get("PYTHONPATH")

print(f"PYTHONPATH = {actual}")
if actual != expected:
    print(f"ERROR: PYTHONPATH should be '{expected}'")
    sys.exit(1)

print("PYTHONPATH OK")

# --- 2. Verify canonical launcher ---
try:
    mod = importlib.import_module("quant.scripts.canonical.canonical_launcher")
    print("Canonical launcher import OK")
except Exception as e:
    print("ERROR importing canonical launcher:", e)
    sys.exit(2)

# --- 3. Verify orchestrator ---
try:
    mod = importlib.import_module("quant.engine.main")
    print("Orchestrator import OK")
except Exception as e:
    print("ERROR importing orchestrator:", e)
    sys.exit(3)

# --- 4. Verify DAG ---
try:
    mod = importlib.import_module("quant.engine.dag")
    print("DAG import OK")
except Exception as e:
    print("ERROR importing DAG:", e)
    sys.exit(4)

# --- 5. Verify task wrappers ---
tasks = [
    "quant.engine.tasks.ingestion",
    "quant.engine.tasks.prices",
    "quant.engine.tasks.returns",
    "quant.engine.tasks.dashboard",
]

for t in tasks:
    try:
        importlib.import_module(t)
        print(f"Task wrapper OK: {t}")
    except Exception as e:
        print(f"ERROR importing task wrapper {t}:", e)
        sys.exit(5)

# --- 6. Verify ingestion backbone ---
try:
    importlib.import_module("quant.ingestion.ingest_5years")
    print("Ingestion backbone import OK")
except Exception as e:
    print("ERROR importing ingestion backbone:", e)
    sys.exit(6)

print("=== ALL IMPORTS OK ===")
sys.exit(0)