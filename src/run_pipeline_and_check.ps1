# C:\Quant\src\run_pipeline_and_check.ps1
$ErrorActionPreference = "Stop"
cd "C:\Quant\src"
python -m quant.engine.orchestrator 2>&1 | Tee-Object -FilePath "C:\Quant\logs\orchestrator_$(Get-Date -Format yyyyMMdd_HHmmss).log"
$prices_max = docker exec -i src-db-1 sh -c 'psql -U quant -d quant -t -c "SELECT TO_CHAR(MAX(date), '\''YYYY-MM-DD'\'') FROM public.prices;"' | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" } | Select-Object -First 1
$returns_max = docker exec -i src-db-1 sh -c 'psql -U quant -d quant -t -c "SELECT TO_CHAR(MAX(as_of), '\''YYYY-MM-DD'\'') FROM returns_daily;"' | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" } | Select-Object -First 1
if ($prices_max -ne $returns_max) {
  Write-Output "Mismatch: prices_max=$prices_max returns_max=$returns_max"
  exit 1
}
Write-Output "OK: $prices_max"
exit 0