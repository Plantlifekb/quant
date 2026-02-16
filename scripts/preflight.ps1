# preflight.ps1
$ErrorActionPreference = "Stop"

$repoRoot = "C:\Quant"
$requiredFiles = @(
    "scripts\logging_quant_v1.py",
    "scripts\canonical\canonical_pipeline_quant_v1.py",
    "scripts\ingestion\ingestion_5years_quant_v1.py",
    "scripts\master_pipeline_quant_v1.py",
    "data\analytics\quant_prices_v1.csv"
)
$requiredDirs = @(
    (Join-Path $repoRoot "data"),
    (Join-Path $repoRoot "logs"),
    (Join-Path $repoRoot "scripts\canonical"),
    (Join-Path $repoRoot "runbook")
)

$missing = @()
foreach ($f in $requiredFiles) {
    $path = Join-Path $repoRoot $f
    if (-not (Test-Path $path)) { $missing += $path }
}
foreach ($d in $requiredDirs) {
    if (-not (Test-Path $d)) { $missing += $d }
}

if ($missing.Count -gt 0) {
    $ts = Get-Date -Format "yyyyMMdd_HHmmss"
    $outDir = Join-Path $repoRoot "runbook"
    New-Item -Path $outDir -ItemType Directory -Force | Out-Null
    $out = Join-Path $outDir "preflight_failure_$ts.txt"
    "PREFLIGHT FAILURE $ts" | Out-File $out -Encoding utf8
    "Missing items:" | Out-File $out -Append -Encoding utf8
    $missing | Out-File $out -Append -Encoding utf8
    Write-Host "PREFLIGHT FAILURE. See $out" -ForegroundColor Red
    exit 1
}

Write-Host "Preflight OK" -ForegroundColor Green
exit 0