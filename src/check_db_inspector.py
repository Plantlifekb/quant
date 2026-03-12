import os
from sqlalchemy import create_engine, inspect, text
url = os.environ.get("DATABASE_URL")
print("DATABASE_URL set:", bool(url))
print("DATABASE_URL:", url)
if not url:
    raise SystemExit(2)
engine = create_engine(url)
print("SQLAlchemy dialect:", engine.dialect.name)
ins = inspect(engine)
# List tables in default schema
tables = ins.get_table_names()
print("Tables found:", tables)
# Check for strategy_selections existence
exists = "strategy_selections" in tables
print("strategy_selections exists:", exists)
# If Postgres, also list materialized views (optional)
if engine.dialect.name == "postgresql":
    with engine.connect() as conn:
        r = conn.execute(text("SELECT matviewname FROM pg_matviews WHERE schemaname='public'")).fetchall()
        print("Materialized views in public:", [row[0] for row in r])
