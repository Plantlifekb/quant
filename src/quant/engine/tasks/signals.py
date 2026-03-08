from sqlalchemy import text

def run(engine):
    with engine.begin() as conn:
        tickers = [row[0] for row in conn.execute(text("SELECT ticker FROM tickers"))]

        for t in tickers:
            rows = conn.execute(
                text("""
                    SELECT date, ret
                    FROM returns
                    WHERE ticker = :t
                    ORDER BY date
                """),
                {"t": t},
            ).fetchall()

            for d, r in rows:
                signal = 1.0 if r > 0 else -1.0

                conn.execute(
                    text("""
                        INSERT INTO signals (ticker, date, signal)
                        VALUES (:t, :d, :s)
                        ON CONFLICT (ticker, date)
                        DO UPDATE SET signal = EXCLUDED.signal
                    """),
                    {"t": t, "d": d, "s": signal},
                )

run.dependencies = ["returns"]