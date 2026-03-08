from sqlalchemy import text

def run(engine):
    with engine.begin() as conn:
        tickers = [row[0] for row in conn.execute(text("SELECT ticker FROM tickers"))]

        for t in tickers:
            rows = conn.execute(
                text("""
                    SELECT date, close
                    FROM prices
                    WHERE ticker = :t
                    ORDER BY date
                """),
                {"t": t},
            ).fetchall()

            # Must have at least 2 rows to compute returns
            if len(rows) < 2:
                continue

            for i in range(1, len(rows)):
                d_prev, p_prev = rows[i - 1]
                d_curr, p_curr = rows[i]

                ret = (p_curr - p_prev) / p_prev

                conn.execute(
                    text("""
                        INSERT INTO returns (ticker, date, ret)
                        VALUES (:t, :d, :r)
                        ON CONFLICT (ticker, date)
                        DO UPDATE SET ret = EXCLUDED.ret
                    """),
                    {"t": t, "d": d_curr, "r": ret},
                )

run.dependencies = ["prices"]