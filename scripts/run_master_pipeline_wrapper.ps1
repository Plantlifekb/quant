# run_master_pipeline_wrapper.ps1
# Hardened launcher for the Quant pipeline (Start-Process for robust Python execution)
$ErrorActionPreference = "Stop"

# --- Configuration (edit if needed) ---
$python = "C:\Users\Keith\AppData\Local\Programs\Python\Python312\python.exe"
$workDir = "C:\Quant"
$scriptDir = Join-Path $workDir "scripts"
$logDir = Join-Path $workDir "logs"
$flagDir = Join-Path $workDir "flags"
$canonicalScript = Join-Path $scriptDir "canonical_launcher.py"
$venvActivate = "C:\Quant\venv\Scripts\Activate.ps1"   # edit if you use a different venv path
$logRetentionDays = 90

# --- Prepare environment ---
If (-not (Test-Path $logDir)) { New-Item -Path $logDir -ItemType Directory | Out-Null }
If (-not (Test-Path $flagDir)) { New-Item -Path $flagDir -ItemType Directory | Out-Null }

$timestamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$logFile = Join-Path $logDir ("master_wrapper_{0}.log" -f $timestamp)

# Force a controlled PYTHONPATH for child processes (no C:\ensemble)
$env:PYTHONPATH = "C:\Quant\scripts;C:\Quant\scripts\canonical;C:\Quant\scripts\logs"

function Log($m) {
  $line = ("[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $m)
  $line | Tee-Object -FilePath $logFile -Append
}

# Start
Log "Wrapper started"
Log ("Effective PYTHONPATH: {0}" -f $env:PYTHONPATH)

# Optional: activate venv if present
if (Test-Path $venvActivate) {
  try {
    Log ("Activating venv: {0}" -f $venvActivate)
    & $venvActivate
  } catch {
    Log ("Failed to activate venv: {0}" -f $_.Exception.Message)
    # continue without venv
  }
}

# --- Preflight: ensure 'ensemble' is not importable ---
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

# --- Run canonical launcher and capture output (robust via Start-Process) ---
if (-not (Test-Path $canonicalScript)) {
  Log ("ERROR: canonical launcher not found at {0}" -f $canonicalScript)
  exit 3
}
Log ("Starting canonical launcher: {0}" -f $canonicalScript)

# Prepare temp files for stdout/stderr
$outTmp = Join-Path $env:TEMP ("canonical_out_{0}.log" -f $timestamp)
$errTmp = Join-Path $env:TEMP ("canonical_err_{0}.log" -f $timestamp)

# Ensure old temp files removed
Remove-Item -Path $outTmp -ErrorAction SilentlyContinue -Force
Remove-Item -Path $errTmp -ErrorAction SilentlyContinue -Force

# Start the Python process and wait
$argList = @($canonicalScript)
$proc = Start-Process -FilePath $pyExe -ArgumentList $argList -RedirectStandardOutput $outTmp -RedirectStandardError $errTmp -NoNewWindow -PassThru -Wait

# Append stdout and stderr to the wrapper log (preserve order: stdout then stderr)
try {
  if (Test-Path $outTmp) {
    Get-Content $outTmp -ErrorAction SilentlyContinue | Tee-Object -FilePath $logFile -Append
  }
  if (Test-Path $errTmp) {
    Get-Content $errTmp -ErrorAction SilentlyContinue | Tee-Object -FilePath $logFile -Append
  }
} catch {
  Log ("Failed to append canonical output to wrapper log: {0}" -f $_.Exception.Message)
}

# Capture exit code
if ($null -ne $proc) {
  $exit = $proc.ExitCode
} else {
  Log "Failed to start canonical process"
  $exit = 5
}

# Clean up temp files
Remove-Item -Path $outTmp -ErrorAction SilentlyContinue -Force
Remove-Item -Path $errTmp -ErrorAction SilentlyContinue -Force

if ($exit -ne 0) {
  Log ("Canonical launcher failed with exit code {0}" -f $exit)
  Log ("See log file: {0}" -f $logFile)
  exit $exit
}

# --- Write success flag with robust logging ---
$flagPath = Join-Path $flagDir "task_success.txt"
try {
  New-Item -Path $flagPath -ItemType File -Force | Out-Null
  Log ("Wrote success flag: {0}" -f $flagPath)
} catch {
  Log ("Failed to write success flag: {0}" -f $_.Exception.Message)
  exit 4
}

Log "Canonical launcher completed successfully"

# --- Rotate old logs ---
try {
  Get-ChildItem -Path $logDir -File -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-1 * $logRetentionDays) } |
    Remove-Item -Force -ErrorAction SilentlyContinue
  Log ("Rotated logs older than {0} days" -f $logRetentionDays)
} catch {
  Log ("Log rotation failed: {0}" -f $_.Exception.Message)
}

exit 0