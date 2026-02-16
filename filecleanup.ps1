<#
QUANT-WIDE ARCHIVAL CLEANUP SCRIPT (SAFE + REVERSIBLE)
------------------------------------------------------
This script:
- Identifies stale files across the entire Quant tree
- Moves them into a single timestamped archive folder
- Preserves only filenames (flat archive)
- Shows a preview before executing
- Requires explicit YES confirmation
#>

Write-Host "=== Quant Cleanup: Discovery Phase ===" -ForegroundColor Cyan

# --- 1. Create timestamped archive folder ---

$timestamp = (Get-Date -Format "yyyyMMdd")
$archiveRoot = "C:\Quant\archive\cleanup_$timestamp"

if (-not (Test-Path $archiveRoot)) {
    New-Item -ItemType Directory -Path $archiveRoot | Out-Null
}

Write-Host "Archive folder: $archiveRoot" -ForegroundColor Yellow

# --- 2. Define stale file rules ---

$root = "C:\Quant"

# Logs older than 7 days
$logFiles = Get-ChildItem "$root\logs" -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-7) }

# Archive data
$archiveData = Get-ChildItem "$root\data\archive" -Recurse -File -ErrorAction SilentlyContinue

# Legacy archive folder (except today's cleanup folder)
$legacyArchive = Get-ChildItem "$root\archive" -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -notlike "$archiveRoot*" }

# .bak and .bak.* files
$bakFiles = Get-ChildItem $root -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match "\.bak(\.|$)" }

# Old attribution outputs
$oldAttribution = Get-ChildItem "$root\data\analytics\attribution_outputs_v1" -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-7) }

# Old reporting outputs
$oldReporting = Get-ChildItem "$root\data\analytics\reporting" -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-7) }

# Old canonical CSVs
$oldCanonical = Get-ChildItem "$root\data\canonical" -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Extension -eq ".csv" }

# Old dashboard pages
$oldPages = Get-ChildItem "$root\archive\pages" -Recurse -File -ErrorAction SilentlyContinue

# Combine all stale files
$staleFiles = @()
$staleFiles += $logFiles
$staleFiles += $archiveData
$staleFiles += $legacyArchive
$staleFiles += $bakFiles
$staleFiles += $oldAttribution
$staleFiles += $oldReporting
$staleFiles += $oldCanonical
$staleFiles += $oldPages

# Remove duplicates
$staleFiles = $staleFiles | Select-Object -Unique

# --- 3. Preview ---

Write-Host "`n=== Stale Files Identified ===" -ForegroundColor Cyan

if ($staleFiles.Count -eq 0) {
    Write-Host "No stale files found. Quant environment is clean." -ForegroundColor Green
    return
}

foreach ($file in $staleFiles) {
    Write-Host "  $($file.FullName)"
}

Write-Host "`nTotal stale files: $($staleFiles.Count)" -ForegroundColor Yellow

# --- 4. Confirmation ---

$confirmation = Read-Host "`nType YES to archive these files"

if ($confirmation -ne "YES") {
    Write-Host "`nCleanup cancelled. No files were archived." -ForegroundColor Green
    return
}

# --- 5. Archive files (flat structure) ---

Write-Host "`nArchiving files..." -ForegroundColor Red

foreach ($file in $staleFiles) {
    try {
        $destination = Join-Path $archiveRoot $file.Name
        Move-Item -Path $file.FullName -Destination $destination -Force
        Write-Host "Archived: $($file.FullName)"
    }
    catch {
        Write-Host "Failed to archive: $($file.FullName)" -ForegroundColor Red
    }
}

Write-Host "`nCleanup complete. All stale files archived to $archiveRoot" -ForegroundColor Green