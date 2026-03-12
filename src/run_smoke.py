@'
#!/usr/bin/env python3
"""Safe smoke runner: tries to call main() in run_ingest, falls back to executing file."""
import importlib.util
import runpy
import sys
from pathlib import Path

ingest_path = Path(__file__).with_name('run_ingest.py')
if ingest_path.exists():
    spec = importlib.util.spec_from_file_location('run_ingest', str(ingest_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore
    if hasattr(module, 'main'):
        try:
            module.main()
        except TypeError:
            module.main()
    else:
        runpy.run_path(str(ingest_path), run_name='__main__')
else:
    print('run_ingest.py not found in src; nothing to run.', file=sys.stderr)
    sys.exit(1)
'@ | Out-File -FilePath src\run_smoke.py -Encoding utf8