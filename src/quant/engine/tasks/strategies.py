from sqlalchemy import text

def run(engine):
    with engine.begin() as conn:
        tickers = [row[0] for row in conn.execute(text("SELECT ticker FROM tickers"))]

        for t in tickers:
            rows = conn.execute(
                text("""
                    SELECT r.date, r.ret, s.signal
                    FROM returns r
                    JOIN signals s
                      ON r.ticker = s.ticker
                     AND r.date = s.date
                    WHERE r.ticker = :t
                    ORDER BY r.date
                """),
                {"t": t},
            ).fetchall()

            for d, r, s in rows:
                pnl = r * s

                conn.execute(
                    text("""
                        INSERT INTO strategies (ticker, date, position, pnl)
                        VALUES (:t, :d, :pos, :pnl)
                        ON CONFLICT (ticker, date)
                        DO UPDATE SET position = EXCLUDED.position,
                                      pnl = EXCLUDED.pnl
                    """),
                    {"t": t, "d": d, "pos": s, "pnl": pnl},
                )

run.dependencies = ["signals"]