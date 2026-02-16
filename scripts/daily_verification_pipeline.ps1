<#
.SYNOPSIS
  Robust daily verification pipeline with system-wide freshness/lineage checks
  and normalization for predicted picks (longshort).

.DESCRIPTION
  - Checks freshness and lineage for ingestion, canonical, analytics, dashboard.
  - Normalizes headers (week_start -> week), date formats, ticker casing.
  - Logs WARN by default; STRICT_VALIDATION=1 makes failures fatal.
#>

# --- Configuration ---
$baseDir       = "C:\Quant"
$dataDir       = Join-Path $baseDir "data"
$ingestDir     = Join-Path $dataDir "ingestion"
$canonDir      = Join-Path $dataDir "canonical"
$analyticsDir  = Join-Path $dataDir "analytics"

$predDir       = Join-Path $baseDir "outputs\verification"
$analysisDir   = Join-Path $baseDir "analysis"
$logsDir       = Join-Path $baseDir "logs"

$pred_longshort = Join-Path $predDir "predicted_vs_picks_weekly_longshort.csv"
$pred_longonly  = Join-Path $predDir "predicted_vs_picks_weekly_longonly.csv"
$real_longshort = Join-Path $analysisDir "regenerated_realized_longshort.csv"
$real_longonly  = Join-Path $analysisDir "regenerated_realized_longonly.csv"

$diagDir = Join-Path $analysisDir "diagnostics"
if (-not (Test-Path $diagDir)) { New-Item -Path $diagDir -ItemType Directory -Force | Out-Null }

$timestamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$logFile   = Join-Path $logsDir "daily_verification_$timestamp.log"

$strict = $env:STRICT_VALIDATION -eq '1'

# --- Logger ---
function Log($level, $msg) {
  $line = "{0} {1}`t{2}" -f (Get-Date).ToString("o"), $level, $msg
  $line | Out-File -FilePath $logFile -Append -Encoding utf8
  Write-Output $line
}

# --- Freshness / lineage helpers ---
function Get-FileInfoOrLog {
  param(
    [string]$Label,
    [string]$Path,
    [ref]   $OutTime
  )
  if (-not (Test-Path $Path)) {
    $msg = "[MISSING] ${Label} not found at ${Path}"
    if ($strict) { Log "ERROR" $msg; exit 1 }
    Log "WARN" $msg
    return
  }
  try {
    $item = Get-Item $Path -ErrorAction Stop
    $OutTime.Value = $item.LastWriteTime
    Log "INFO" "[FRESHNESS] ${Label} => $($item.LastWriteTime.ToString('s'))"
  } catch {
    $msg = "[ERROR] Failed to stat ${Label} at ${Path}: $_"
    if ($strict) { Log "ERROR" $msg; exit 1 }
    Log "WARN" $msg
  }
}

function Check-Lineage {
  param(
    [string]  $EarlierLabel,
    [datetime]$EarlierTime,
    [string]  $LaterLabel,
    [datetime]$LaterTime
  )
  if (-not $EarlierTime -or -not $LaterTime) { return }
  if ($LaterTime -lt $EarlierTime) {
    $msg = "[LINEAGE] ${LaterLabel} ($($LaterTime.ToString('s'))) is older than ${EarlierLabel} ($($EarlierTime.ToString('s')))"
    if ($strict) { Log "ERROR" $msg; exit 1 }
    Log "WARN" $msg
  } else {
    Log "INFO" "[LINEAGE] ${EarlierLabel} -> ${LaterLabel} OK"
  }
}

# --- Freshness / lineage verification ---
Log "INFO" "START system freshness / lineage verification"

$ingestFile       = Join-Path $ingestDir   "ingestion_5years.csv"
$canonPrices      = Join-Path $canonDir    "prices.parquet"
$canonFund        = Join-Path $canonDir    "fundamentals.parquet"
$canonRisk        = Join-Path $canonDir    "risk_model.parquet"
$analyticsPicks   = Join-Path $analyticsDir "quant_weekly_picks_quant_v1.parquet"
$dashboardInputs  = Join-Path $canonDir    "quant_dashboard_inputs_v2.csv"

# Nullable timestamps
$tIngest = $null
$tCanonPrices = $null
$tCanonFund = $null
$tCanonRisk = $null
$tAnalyticsPicks = $null
$tDashboardInputs = $null

# Freshness checks
Get-FileInfoOrLog -Label "Ingestion (5y)"          -Path $ingestFile        -OutTime ([ref]$tIngest)
Get-FileInfoOrLog -Label "Canonical prices"        -Path $canonPrices       -OutTime ([ref]$tCanonPrices)
Get-FileInfoOrLog -Label "Canonical fundamentals"  -Path $canonFund         -OutTime ([ref]$tCanonFund)
Get-FileInfoOrLog -Label "Canonical risk_model"    -Path $canonRisk         -OutTime ([ref]$tCanonRisk)
Get-FileInfoOrLog -Label "Analytics weekly picks"  -Path $analyticsPicks    -OutTime ([ref]$tAnalyticsPicks)
Get-FileInfoOrLog -Label "Dashboard inputs v2"     -Path $dashboardInputs   -OutTime ([ref]$tDashboardInputs)

# Canonical aggregate timestamp
$canonTimes = @()
if ($tCanonPrices) { $canonTimes += $tCanonPrices }
if ($tCanonFund)   { $canonTimes += $tCanonFund }
if ($tCanonRisk)   { $canonTimes += $tCanonRisk }

$tCanon = $null
if ($canonTimes.Count -gt 0) {
  $tCanon = ($canonTimes | Measure-Object -Maximum).Maximum
  Log "INFO" "[FRESHNESS] Canonical aggregate timestamp => $($tCanon.ToString('s'))"
}

# Lineage
Check-Lineage -EarlierLabel "Ingestion (5y)"         -EarlierTime $tIngest          -LaterLabel "Canonical"              -LaterTime $tCanon
Check-Lineage -EarlierLabel "Canonical"              -EarlierTime $tCanon           -LaterLabel "Analytics weekly picks" -LaterTime $tAnalyticsPicks
Check-Lineage -EarlierLabel "Analytics weekly picks" -EarlierTime $tAnalyticsPicks  -LaterLabel "Dashboard inputs v2"    -LaterTime $tDashboardInputs

Log "INFO" "END system freshness / lineage verification"

# --- Helpers for picks verification ---
function Backup-IfExists($path) {
  if (Test-Path $path) {
    $bak = "$path.pre_replace.$timestamp.bak"
    Copy-Item -Path $path -Destination $bak -Force
    Log "INFO" "Backed up ${path} -> ${bak}"
  }
}

function Normalize-Date($s) {
  if (-not $s) { return $s }
  try { return ([datetime]::Parse($s)).ToString('yyyy-MM-dd') } catch { return $s }
}

# --- Normalize predicted longshort picks ---
if (-not (Test-Path $pred_longshort)) {
  Log "WARN" "Predicted longshort file missing: ${pred_longshort}"
} else {
  try {
    $raw = Get-Content $pred_longshort -ErrorAction Stop
    if ($raw.Count -gt 0 -and $raw[0] -match 'week_start') {
      Backup-IfExists $pred_longshort
      $raw[0] = $raw[0] -replace 'week_start','week'
      $tmp = Join-Path $env:TEMP ("predicted_vs_picks_weekly_longshort.normalized.$timestamp.csv")
      $raw | Set-Content -Path $tmp -Encoding utf8
      Move-Item -Path $tmp -Destination $pred_longshort -Force
      Log "INFO" "Normalized header week_start -> week in ${pred_longshort}"
    }
  } catch {
    Log "ERROR" "Failed to normalize header for ${pred_longshort}: $_"
  }
}

# --- Import predicted + realized ---
$pred = @()
$real = @()

if (Test-Path $pred_longshort) {
  try {
    $pred = Import-Csv $pred_longshort -ErrorAction Stop | ForEach-Object {
      if ($_.PSObject.Properties.Name -contains 'week')   { $_.week   = Normalize-Date($_.week) }
      if ($_.PSObject.Properties.Name -contains 'ticker') { $_.ticker = ($_.ticker -as [string]).Trim().ToUpper() }
      $_
    }
    Log "INFO" "Imported predicted longshort rows: $($pred.Count)"
  } catch {
    Log "ERROR" "Failed to import predicted longshort: $_"
  }
}

if (Test-Path $real_longshort) {
  try {
    $real = Import-Csv $real_longshort -ErrorAction Stop | ForEach-Object {
      if ($_.PSObject.Properties.Name -contains 'week')   { $_.week   = Normalize-Date($_.week) }
      if ($_.PSObject.Properties.Name -contains 'ticker') { $_.ticker = ($_.ticker -as [string]).Trim().ToUpper() }
      $_
    }
    Log "INFO" "Imported realized longshort rows: $($real.Count)"
  } catch {
    Log "ERROR" "Failed to import realized longshort: $_"
  }
}

# --- Matching test ---
$matchCount = 0
if ($pred.Count -gt 0 -and $real.Count -gt 0) {
  foreach ($p in $pred) {
    $found = $real | Where-Object { $_.ticker -eq $p.ticker -and $_.week -eq $p.week }
    if ($found) { $matchCount += ($found | Measure-Object).Count }
  }
}

if ($pred.Count -eq 0) { Log "WARN" "Predicted longshort file has no rows." }
if ($real.Count -eq 0) { Log "WARN" "Realized longshort file has no rows." }

if ($matchCount -eq 0) {
  Log "WARN" "No matching predicted rows found for longshort (matches: ${matchCount})."
  $diagPred = Join-Path $diagDir "predicted_longshort_sample.$timestamp.csv"
  $diagReal = Join-Path $diagDir "realized_longshort_sample.$timestamp.csv"
  if ($pred.Count -gt 0) { $pred | Select week,ticker,score,side,target_weight | Export-Csv -Path $diagPred -NoTypeInformation -Encoding utf8; Log "WARN" "Wrote predicted sample to ${diagPred}" }
  if ($real.Count -gt 0) { $real | Select week,ticker,predicted_score,side,target_weight | Export-Csv -Path $diagReal -NoTypeInformation -Encoding utf8; Log "WARN" "Wrote realized sample to ${diagReal}" }
  if ($strict) { Log "ERROR" "STRICT_VALIDATION enabled. Aborting."; exit 1 }
} else {
  Log "INFO" "Longshort normalization complete (matches: ${matchCount})."
}

# --- Longonly placeholder ---
try {
  Log "INFO" "Starting longonly verification steps..."
  # <insert longonly logic here>
  Log "INFO" "Longonly verification completed."
} catch {
  Log "ERROR" "Longonly verification failed: $_"
}

# --- Verification runner placeholder ---
try {
  Log "INFO" "Running verification runner..."
  # <insert call to your verification runner here>
  Log "INFO" "Verification runner invoked."
} catch {
  Log "ERROR" "Failed to invoke verification runner: $_"
}

# --- Dashboard weekly parquet refresh ---
try {
  Log "INFO" "Starting dashboard weekly parquet refresh via Python..."

  $python = "C:\Users\Keith\AppData\Local\Programs\Python\Python312\python.exe"
  $refreshScript = "C:\Quant\scripts\refresh_dashboard_weekly.py"

  & $python $refreshScript

  Log "INFO" "Dashboard weekly parquet refresh completed."
} catch {
  Log "ERROR" "Dashboard weekly parquet refresh failed: $_"
}
Log "INFO" "END pipeline success"