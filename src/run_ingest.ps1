# C:\Quant\src\run_ingest.ps1
# Set Postgres env vars for the job
$env:PGHOST = "localhost"
$env:PGPORT = "5432"
$env:PGDATABASE = "quant"
$env:PGUSER = "quant"
$env:PGPASSWORD = "quant"

# Optional: activate virtualenv if you use one
# $venv = "C:\path\to\venv\Scripts\Activate.ps1"
# if (Test-Path $venv) { & $venv }

# Start transcript to capture stdout/stderr and scheduler output
$transcript = "C:\Quant\src\run.log"
Start-Transcript -Path $transcript -Append

# Run the Python runner
python "C:\Quant\src\run_ingest.py"
$exit = $LASTEXITCODE

Stop-Transcript
exit $exit
