# --- Hardened Quant Pipeline Wrapper ---

$ErrorActionPreference = "Stop"

# --- Environment ---
$env:PYTHONPATH = "C:\Quant\src"
$env:DATABASE_URL = "postgresql+psycopg2://quant:quant@localhost:5432/quant"

# --- Paths ---
$python = "C:\Quant\.venv\Scripts\python.exe"
$launcher = "C:\Quant\src\quant\engine\launcher.py"
$logDir = "C:\Quant\logs"
$timestamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$logFile = Join-Path $logDir ("pipeline_run_{0}.log" -f $timestamp)

# --- Logging helper ---
function Log($msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    $line | Tee-Object -FilePath $logFile -Append
}

# --- Start ---
Log "Wrapper started"
Log "PYTHONPATH=$env:PYTHONPATH"
Log "DATABASE_URL=$env:DATABASE_URL"
Log "Launcher=$launcher"

# --- Run orchestrator cleanly ---
$processOutput = cmd.exe /c "`"$python`" `"$launcher`" 2>&1"
$exitCode = $LASTEXITCODE

$processOutput | Tee-Object -FilePath $logFile -Append

if ($exitCode -ne 0) {
    Log "Pipeline FAILED with exit code $exitCode"
    exit $exitCode
}

Log "Pipeline completed successfully"
exit 0