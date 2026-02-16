# PowerShell wrapper for weekly refresh
Write-Host "=== Weekly Returns Refresh ==="

$python = "C:\Users\Keith\AppData\Local\Programs\Python\Python312\python.exe"
$script = "C:\Quant\scripts\refresh_weekly_returns.py"

& $python $script

Write-Host "=== Weekly Refresh Complete ==="