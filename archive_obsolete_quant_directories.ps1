# Root of the real quant repo
$repoRoot = "C:\Quant\src\quant"

# Archive root
$archiveRoot = "C:\archive"

# Ensure archive root exists
if (!(Test-Path $archiveRoot)) {
    New-Item -ItemType Directory -Path $archiveRoot | Out-Null
}

# Directories that are obsolete based on your real tree
$obsoleteDirs = @(
    "forward_returns",
    "ingestion",
    "prices",
    "returns",
    "signals",
    "strategy",
    "scripts",
    "__pycache__"
)

foreach ($dir in $obsoleteDirs) {
    $fullPath = Join-Path $repoRoot $dir

    if (Test-Path $fullPath) {
        $archivePath = Join-Path $archiveRoot "quant\$dir"
        $archiveDir = Split-Path $archivePath

        if (!(Test-Path $archiveDir)) {
            New-Item -ItemType Directory -Path $archiveDir -Force | Out-Null
        }

        Move-Item -Path $fullPath -Destination $archivePath -Force
        Write-Host "Archived directory: $dir"
    }
    else {
        Write-Host "Not found (skipped): $dir"
    }
}