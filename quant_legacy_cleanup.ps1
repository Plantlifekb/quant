<#
QUANT LEGACY CLEANUP (STRICT MODE)
----------------------------------
Moves all NON-ACTIVE files (legacy v1 + stale RED files) into:

    C:\Quant\archive\legacy_cleanup_YYYYMMDD\

ACTIVE files remain untouched:
- ingestion
- canonical parquet
- dashboard v2
- verification
- weekly picks v2
- event engine
- logs from today
- cleanup scripts
- freshness audit

This script is SAFE, REVERSIBLE, and PREVIEW-FIRST.
#>

Write-Host "=== Quant Legacy Cleanup (Strict Mode) ===" -ForegroundColor Cyan

$root = "C:\Quant"
$today = Get-Date
$cutoff = (Get-Date "2026-01-26")   # strict boundary for legacy v1

# Create archive folder
$timestamp = $today.ToString("yyyyMMdd")
$archiveRoot = "C:\Quant\archive\legacy_cleanup_$timestamp"

if (-not (Test-Path $archiveRoot)) {
    New-Item -ItemType Directory -Path $archiveRoot | Out-Null
}

Write-Host "Archive folder: $archiveRoot" -ForegroundColor Yellow

# --- ACTIVE PATHS (never archive) ---
$activePaths = @(
    "\data\canonical",
    "\data\analytics\reporting",
    "\data\analytics\dashboard",
    "\data\analytics\event",
    "\data\analytics\verification",
    "\data\analytics\weekly",
    "\data\analytics\predicted",
    "\data\analytics\realized",
    "\data\analytics\positions",
    "\scripts\dashboard",
    "\scripts\verification",
    "\scripts\ingestion",
    "\scripts\event",
    "\scripts\weekly",
    "\scripts\cleanup",
    "\scripts\freshness",
    "\logs\" + $today.ToString("yyyyMMdd")
)

function Is-ActiveFile {
    param([string]$path)

    foreach ($active in $activePaths) {
        if ($path.ToLower().Contains($active.ToLower())) {
            return $true
        }
    }

    # canonical parquet always active
    if ($path.ToLower().EndsWith(".parquet")) { return $true }

    return $false
}

# --- Identify NON-ACTIVE files ---
$allFiles = Get-ChildItem $root -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -notlike "*\archive\cleanup_*" }

$nonActive = foreach ($f in $allFiles) {

    # Skip active files
    if (Is-ActiveFile $f.FullName) { continue }

    # Strict rule: legacy v1 = older than 26 Jan 2026
    if ($f.LastWriteTime -gt $cutoff) { continue }

    # Otherwise: this is a NON-ACTIVE legacy file
    $f
}

# --- Preview ---
Write-Host "`n=== NON-ACTIVE FILES IDENTIFIED ===" -ForegroundColor Cyan

if ($nonActive.Count -eq 0) {
    Write-Host "No legacy files found. System is fully active." -ForegroundColor Green
    return
}

foreach ($file in $nonActive) {
    Write-Host "  $($file.FullName)"
}

Write-Host "`nTotal non-active legacy files: $($nonActive.Count)" -ForegroundColor Yellow

# --- Confirmation ---
$confirmation = Read-Host "`nType YES to archive these legacy files"

if ($confirmation -ne "YES") {
    Write-Host "`nCleanup cancelled. No files were archived." -ForegroundColor Green
    return
}

# --- Archive ---
Write-Host "`nArchiving legacy files..." -ForegroundColor Red

foreach ($file in $nonActive) {
    try {
        $destination = Join-Path $archiveRoot $file.Name
        Move-Item -Path $file.FullName -Destination $destination -Force
        Write-Host "Archived: $($file.FullName)"
    }
    catch {
        Write-Host "Failed to archive: $($file.FullName)" -ForegroundColor Red
    }
}

Write-Host "`nLegacy cleanup complete. Files archived to $archiveRoot" -ForegroundColor Green