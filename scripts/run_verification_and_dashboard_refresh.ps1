# Run the daily verification pipeline
& "C:\Quant\scripts\daily_verification_pipeline.ps1"

# Refresh the dashboard weekly parquet
$python = "C:\Users\Keith\AppData\Local\Programs\Python\Python312\python.exe"
$refreshScript = "C:\Quant\scripts\refresh_dashboard_weekly.py"
& $python $refreshScript