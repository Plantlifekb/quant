$env:DATABASE_URL = "sqlite:///C:/Quant/data/quant_dev.sqlite"
Set-Location "C:\Quant"
& "C:\Quant\.venv\Scripts\python.exe" "C:\Quant\src\quant\engine\launcher.py"
