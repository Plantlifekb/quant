# Quant Directory Audit (Read-Only)

$root = "C:\Quant"

Write-Host "=== Quant Directory Audit ===" -ForegroundColor Cyan

Get-ChildItem -Path $root -Recurse -File |
    Select-Object `
        FullName,
        @{Name="LastWrite";Expression={$_.LastWriteTime}},
        @{Name="AgeDays";Expression={(New-TimeSpan -Start $_.LastWriteTime -End (Get-Date)).Days}},
        Length |
    Sort-Object AgeDays -Descending |
    Format-Table -AutoSize
