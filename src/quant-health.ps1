Write-Host "=== QUANT HEALTH CHECK ==="
Write-Host ""   # Prevent PowerShell from misinterpreting Python output

$engine = "postgresql://quant:quant@localhost:5432/quant"

# 1. Verify DB connection
Write-Host "`nChecking DB connection..."
python -c "
from sqlalchemy import create_engine, text
e = create_engine('$engine')
with e.connect() as c:
    print('Connected:', c.execute(text('SELECT 1')).scalar())
"

# 2. List tables
Write-Host "`nListing tables..."
python -c "
from sqlalchemy import create_engine, inspect
e = create_engine('$engine')
insp = inspect(e)
print(sorted(insp.get_table_names()))
"

# 3. Row counts
Write-Host "`nRow counts..."
python -c "
from sqlalchemy import create_engine, text
e = create_engine('$engine')
tables = ['prices','fundamentals','task_metadata','task_run_history']
with e.connect() as c:
    for t in tables:
        count = c.execute(text(f'SELECT COUNT(*) FROM {t}')).scalar()
        print(t, count)
"

# 4. Latest timestamps (schema‑correct)
Write-Host "`nLatest timestamps..."
python -c "
from sqlalchemy import create_engine, text
e = create_engine('$engine')
queries = [
    ('prices','date'),
    ('fundamentals','as_of_date')
]
with e.connect() as c:
    for table, col in queries:
        ts = c.execute(text(f'SELECT MAX({col}) FROM {table}')).scalar()
        print(table, ts)
"

# 5. Orchestrator tasks (schema‑correct)
Write-Host "`nOrchestrator tasks..."
python -c "
from sqlalchemy import create_engine, text
e = create_engine('$engine')
with e.connect() as c:
    rows = c.execute(text('SELECT * FROM task_metadata ORDER BY task_name'))
    for r in rows:
        print(r)
"

# 6. Recent task runs (schema‑correct)
Write-Host "`nRecent task runs..."
python -c "
from sqlalchemy import create_engine, text
e = create_engine('$engine')
with e.connect() as c:
    rows = c.execute(text('SELECT task_name, status, run_started, run_finished, error_text FROM task_run_history ORDER BY run_started DESC LIMIT 10'))
    for r in rows:
        print(r)
"

Write-Host "=== QUANT HEALTH CHECK COMPLETE ==="