$env:PYTHONPATH = "C:\Quant\src"
$env:DATABASE_URL = "sqlite:///C:/Quant/data/quant_dev.sqlite"
Start-Process -FilePath "C:\Quant\.venv\Scripts\python.exe" -ArgumentList "C:\Quant\src\quant\dashboard\main.py" -WorkingDirectory "C:\Quant\src" -WindowStyle Hidden
