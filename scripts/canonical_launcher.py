import os
import sys
import traceback

# canonical_launcher.py (debugging/hard-bootstrap)
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_scripts = os.path.join(_repo_root, "scripts")
_scripts_logs = os.path.join(_scripts, "logs")

# Ensure scripts paths are first on sys.path
for _p in (_scripts, _scripts_logs):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Emit diagnostic info to stdout (captured by wrapper into canonical log)
print("LAUNCHER DIAG: repo_root=", _repo_root)
print("LAUNCHER DIAG: scripts=", _scripts)
print("LAUNCHER DIAG: scripts_logs=", _scripts_logs)
print("LAUNCHER DIAG: sys.path head:", sys.path[:6])

# Check that the files exist on disk
print("LAUNCHER DIAG: exists(scripts)=", os.path.exists(_scripts))
print("LAUNCHER DIAG: exists(scripts_logs)=", os.path.exists(_scripts_logs))

# Try importing logging_quant_v1 and report result
try:
    import logging_quant_v1
    print("LAUNCHER DIAG: imported logging_quant_v1 from", getattr(logging_quant_v1, "__file__", "<built-in>"))
except Exception as e:
    print("LAUNCHER DIAG: failed to import logging_quant_v1:", repr(e))
    traceback.print_exc()

# Execute the canonical script as __main__
canonical_path = os.path.join(_scripts, "canonical", "canonical_pipeline_quant_v1.py")
print("LAUNCHER DIAG: executing canonical at", canonical_path)
with open(canonical_path, "r", encoding="utf-8") as f:
    code = f.read()
exec(compile(code, canonical_path, "exec"), {"__name__": "__main__"})
