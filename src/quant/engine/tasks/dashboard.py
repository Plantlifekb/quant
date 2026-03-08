from sqlalchemy import text

def run(engine):
    with engine.begin() as conn:
        total_pnl = conn.execute(
            text("""
                SELECT SUM(pnl) 
                FROM strategies
            """)
        ).scalar() or 0.0

        conn.execute(
            text("""
                INSERT INTO dashboard_summary (metric, value)
                VALUES ('total_pnl', :v)
                ON CONFLICT (metric)
                DO UPDATE SET value = EXCLUDED.value
            """),
            {"v": total_pnl},
        )

run.dependencies = ["strategies"]