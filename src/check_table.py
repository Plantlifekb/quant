import os
from sqlalchemy import create_engine, text
url = os.environ.get("DATABASE_URL")
print("DATABASE_URL set:", bool(url))
if not url:
    raise SystemExit(2)
engine = create_engine(url)
with engine.connect() as conn:
    r = conn.execute(text("SELECT to_regclass('public.strategy_selections')")).scalar()
    print("to_regclass('public.strategy_selections') ->", r)
