<#
QUANT FRESHNESS AUDIT
---------------------
This script scans the Quant directory and produces a detailed table showing:

- File name
- Expected cadence (Daily / Weekly / Static / Log)
- Last updated timestamp
- Status (GREEN / YELLOW / RED)

Cadence is inferred automatically from file paths and naming patterns.
This script is READ-ONLY and makes no changes to the filesystem.
#>

Write-Host "=== Quant Freshness Audit ===" -ForegroundColor Cyan

$root = "C:\Quant"

# --- Helper: infer cadence based on path + filename ---
function Get-Cadence {
    param([string]$path)

    $lower = $path.ToLower()

    if ($lower -like "*\logs\*") { return "Log" }
    if ($lower -like "*\data\canonical\*") { return "Static" }
    if ($lower -like "*\data\archive\*") { return "Archive" }
    if ($lower -like "*\attribution_outputs_v1\*") { return "Weekly" }
    if ($lower -like "*\reporting\*") { return "Daily" }
    if ($lower -like "*\portfolio\*") { return "Daily" }
    if ($lower -like "*\risk\*") { return "Daily" }
    if ($lower -like "*\analytics\*") { return "Daily" }

    # Naming patterns
    if ($lower -match "rolling|summary|pnl") { return "Weekly" }
    if ($lower -match "v1|v2|timeseries|signals|report") { return "Daily" }

    return "Daily"
}

# --- Helper: determine freshness threshold ---
function Get-Threshold {
    param([string]$cadence)

    switch ($cadence) {
        "Daily"  { return (Get-Date).AddDays(-1) }
        "Weekly" { return (Get-Date).AddDays(-7) }
        "Static" { return (Get-Date).AddYears(-10) } # static files rarely change
        "Log"    { return (Get-Date).AddDays(-1) }
        default  { return (Get-Date).AddDays(-1) }
    }
}

# --- Helper: determine status ---
function Get-Status {
    param(
        [datetime]$lastWrite,
        [datetime]$threshold
    )

    if ($lastWrite -ge $threshold) { return "GREEN" }
    if ($lastWrite -ge $threshold.AddDays(-3)) { return "YELLOW" }
    return "RED"
}

# --- Collect all relevant files ---
$files = Get-ChildItem $root -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object {
        $_.FullName -notlike "*\archive\cleanup_*"  # ignore cleanup archives
    }

# --- Build audit table ---
$audit = foreach ($f in $files) {

    $cadence = Get-Cadence -path $f.FullName
    $threshold = Get-Threshold -cadence $cadence
    $status = Get-Status -lastWrite $f.LastWriteTime -threshold $threshold

    [PSCustomObject]@{
        FileName        = $f.Name
        Path            = $f.FullName
        Cadence         = $cadence
        LastUpdated     = $f.LastWriteTime
        Threshold       = $threshold
        Status          = $status
    }
}

# --- Output table ---
Write-Host "`n=== Freshness Report ===" -ForegroundColor Cyan

$audit |
    Sort-Object Status, Cadence, LastUpdated |
    Format-Table FileName, Cadence, LastUpdated, Status -AutoSize

Write-Host "`nAudit complete." -ForegroundColor Green