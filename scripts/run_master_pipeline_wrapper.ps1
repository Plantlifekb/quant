# run_master_pipeline_wrapper.ps1
# Minimal hardened launcher for the Quant pipeline
$ErrorActionPreference = "Stop"

# --- Configuration ---
$python = "C:\Users\Keith\AppData\Local\Programs\Python\Python312\python.exe"
$workDir = "C:\Quant"
$scriptDir = Join-Path $workDir "scripts"
$logDir = Join-Path $workDir "logs"
$flagDir = Join-Path $workDir "flags"
$canonicalScript = Join-Path $scriptDir "canonical_launcher.py"
$timestamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$logFile = Join-Path $logDir ("master_wrapper_{0}.log" -f $timestamp)

# --- Ensure directories exist ---
If (-not (Test-Path $logDir)) { New-Item -Path $logDir -ItemType Directory | Out-Null }
If (-not (Test-Path $flagDir)) { New-Item -Path $flagDir -ItemType Directory | Out-Null }

# --- Controlled runtime environment ---
# Force a controlled PYTHONPATH for child processes (no C:\ensemble)
$env:PYTHONPATH = "C:\Quant\scripts;C:\Quant\scripts\canonical;C:\Quant\scripts\logs"

# Optional virtualenv activation (uncomment and edit if you use a venv)
# $venvActivate = "C:\Quant\venv\Scripts\Activate.ps1"
# if (Test-Path $venvActivate) { & $venvActivate }

# --- Simple logger helper ---
function Log($m) {
  $line = ("[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $m)
  $line | Tee-Object -FilePath $logFile -Append
}

# Start log
Log "Wrapper started"
Log ("Effective PYTHONPATH: {0}" -f $env:PYTHONPATH)

# --- Preflight: ensure abandoned package not importable ---
$pyExe = $python
$preflightPath = Join-Path $env:TEMP ("quant_preflight_{0}.py" -f $timestamp)
$preflightCode = @'
import importlib.util, sys, os
spec = importlib.util.find_spec("ensemble")
print("PYTHONPATH=", os.environ.get("PYTHONPATH"))
if spec is not None:
    print("ERROR: 'ensemble' is importable at runtime:", spec)
    sys.exit(2)
print("Preflight OK: 'ensemble' not importable")
'@
Set-Content -Path $preflightPath -Value $preflightCode -Encoding UTF8
Log ("Running preflight python: {0}" -f $preflightPath)
& $pyExe $preflightPath 2>&1 | Tee-Object -FilePath $logFile -Append
$pfExit = $LASTEXITCODE
Remove-Item -Path $preflightPath -Force -ErrorAction SilentlyContinue
if ($pfExit -ne 0) {
  Log ("Preflight failed with exit code {0}" -f $pfExit)
  Log "Wrapper exiting due to preflight failure"
  exit $pfExit
}
Log "Preflight OK"

# --- Run canonical launcher and capture output ---
if (-not (Test-Path $canonicalScript)) {
  Log ("ERROR: canonical launcher not found at {0}" -f $canonicalScript)
  exit 3
}
Log ("Starting canonical launcher: {0}" -f $canonicalScript)
& $pyExe $canonicalScript 2>&1 | Tee-Object -FilePath $logFile -Append
$exit = $LASTEXITCODE

if ($exit -eq 0) {
  # Success flag
  New-Item -Path (Join-Path $flagDir "task_success.txt") -ItemType File -Force | Out-Null
  Log "Canonical launcher completed successfully"
  exit 0
} else {
  Log ("Canonical launcher failed with exit code {0}" -f $exit)
  Log ("See log file: {0}" -f $logFile)
  exit $exit
}