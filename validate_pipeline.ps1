# validate_pipeline.ps1
param(
  [string]$Snapshot = "data/canonical",
  [string]$LogPath = "tmp/pipeline.log"
)

# Ensure script runs from repo root
Set-Location -Path (Split-Path -Path $MyInvocation.MyCommand.Definition -Parent)
New-Item -ItemType Directory -Path tmp -Force | Out-Null

Write-Host "Running master pipeline (snapshot: $Snapshot) ..."
python -u scripts/master_pipeline_quant_v1.py --snapshot $Snapshot > $LogPath 2>&1
Write-Host "Pipeline exit code: $LASTEXITCODE"
Write-Host "=== pipeline log tail ==="
Get-Content $LogPath -Tail 200 | ForEach-Object { Write-Host $_ }

$logText = Get-Content $LogPath -Raw -ErrorAction SilentlyContinue
if (-not ($logText -match "MASTER PIPELINE COMPLETED SUCCESSFULLY")) {
    Write-Error "Pipeline did not complete successfully. See $LogPath"
    exit 2
}

$artifacts = @(
    "data/analytics/quant_prices_v1.csv",
    "data/analytics/strategy_returns.parquet",
    "data/analytics/quant_weekly_picks_quant_v1.parquet",
    "data/analytics/weekly_selection_canonical.csv"
)

$found = $false
foreach ($a in $artifacts) {
    if (Test-Path $a) {
        Write-Host "Found artifact: $a"
        $found = $true
    }
}

if (-not $found) {
    Write-Error "No expected artifacts found. Listing data/analytics:"
    Get-ChildItem data\analytics -Force | Select-Object Name, LastWriteTime | Format-Table
    exit 3
}

Write-Host "Validation succeeded: pipeline completed and artifacts found."
exit 0
