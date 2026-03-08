from quant.common.db import create_db_engine

engine = create_db_engine()

with engine.connect() as conn:
    result = conn.exec_driver_sql(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
    )
    print(result.fetchall())