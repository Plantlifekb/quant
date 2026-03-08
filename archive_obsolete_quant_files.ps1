# Root of the real quant repo
$repoRoot = "C:\Quant\src\quant"

# Archive root
$archiveRoot = "C:\archive"

# Ensure archive root exists
if (!(Test-Path $archiveRoot)) {
    New-Item -ItemType Directory -Path $archiveRoot | Out-Null
}

# Obsolete files
$obsoleteFiles = @(
    "returns_daily.py",
    "db.py"
)

foreach ($file in $obsoleteFiles) {
    $fullPath = Join-Path $repoRoot $file

    if (Test-Path $fullPath) {
        $archivePath = Join-Path $archiveRoot "quant\$file"
        $archiveDir = Split-Path $archivePath

        if (!(Test-Path $archiveDir)) {
            New-Item -ItemType Directory -Path $archiveDir -Force | Out-Null
        }

        Move-Item -Path $fullPath -Destination $archivePath -Force
        Write-Host "Archived file: $file"
    }
    else {
        Write-Host "Not found (skipped): $file"
    }
}