from sqlalchemy import text
import datetime
import math

def run(engine):
    start = datetime.date(2020, 1, 1)
    end = datetime.date(2020, 12, 31)

    with engine.begin() as conn:
        tickers = [r[0] for r in conn.execute(text("SELECT ticker FROM tickers"))]

        for t in tickers:
            for i in range((end - start).days + 1):
                d = start + datetime.timedelta(days=i)
                price = 100 + 10 * math.sin(i / 20.0)

                conn.execute(
                    text("""
                        INSERT INTO prices (ticker, date, close)
                        VALUES (:t, :d, :p)
                        ON CONFLICT (ticker, date)
                        DO UPDATE SET close = EXCLUDED.close
                    """),
                    {"t": t, "d": d, "p": price},
                )

run.dependencies = ["ingestion"]