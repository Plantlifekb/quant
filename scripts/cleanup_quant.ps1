# cleanup_quant.ps1
$Root = "C:\Quant"
$Cutoff = (Get-Date).AddDays(-7)
$Archive = Join-Path $Root "archive\cleanup_$(Get-Date -Format yyyyMMdd)"
New-Item -ItemType Directory -Path $Archive -Force | Out-Null

# Preview what will be archived
Write-Output "Archiving files older than 7 days from $Root to $Archive"
Get-ChildItem -Path $Root -Recurse -File -Include auto_*,tmp_*,scratch_* |
  Where-Object { $_.LastWriteTime -lt $Cutoff } |
  Select-Object FullName,Length,LastWriteTime |
  Format-Table -AutoSize | Out-String | Write-Output

# Move matching files into the dated archive
Get-ChildItem -Path $Root -Recurse -File -Include auto_*,tmp_*,scratch_* |
  Where-Object { $_.LastWriteTime -lt $Cutoff } |
  ForEach-Object {
    $dest = Join-Path $Archive $_.Name
    Move-Item -Path $_.FullName -Destination $dest -Force -ErrorAction SilentlyContinue
    Write-Output ("Moved: {0} -> {1}" -f $_.FullName, $dest)
  }

# Write a simple run log
$log = Join-Path $Archive "cleanup_log_$(Get-Date -Format yyyyMMdd_HHmmss).txt"
"Cleanup run at $(Get-Date)" | Out-File -FilePath $log -Encoding utf8
Write-Output "Cleanup finished. Log: $log"