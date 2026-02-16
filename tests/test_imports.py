import importlib, sys, os

repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
scripts_dir = os.path.join(repo_root, "scripts")
logs_dir = os.path.join(scripts_dir, "logs")
for p in (scripts_dir, logs_dir):
    if p not in sys.path:
        sys.path.insert(0, p)

modules = [
    "logging_quant_v1",
    "canonical.canonical_pipeline_quant_v1",
    "ingestion.ingestion_5years_quant_v1",
    "master_pipeline_quant_v1"
]

failed = []
for m in modules:
    try:
        importlib.import_module(m)
    except Exception as e:
        failed.append((m, repr(e)))

if failed:
    for m, e in failed:
        print(f"IMPORT FAIL: {m} -> {e}")
    raise SystemExit(1)

print("IMPORTS OK")