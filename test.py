# create the test script safely using a PowerShell here-string
Set-Content -Path .\tmp_db_test.py -Value @'
import os
from sqlalchemy import create_engine

url = os.environ.get("DATABASE_URL")
e = create_engine(url)
with e.connect() as conn:
    print("DB connect OK")
'@

# run it with the venv Python
& '.\.venv\Scripts\python.exe' .\tmp_db_test.py

# remove the script (optional)
Remove-Item .\tmp_db_test.py
