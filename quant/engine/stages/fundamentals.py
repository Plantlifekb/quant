from datetime import date
from sqlalchemy import text

def run(engine):
    today = date.today()

    with engine.begin() as conn:
        tickers = [
            row[0]
            for row in conn.execute(text("SELECT ticker FROM tickers ORDER BY ticker"))
        ]

        for t in tickers:
            conn.execute(
                text(
                    """
                    INSERT INTO fundamentals (
                        ticker,
                        as_of_date,
                        revenue,
                        earnings,
                        book_value,
                        eps,
                        pe_ratio,
                        pb_ratio
                    )
                    VALUES (
                        :ticker,
                        :as_of_date,
                        :revenue,
                        :earnings,
                        :book_value,
                        :eps,
                        :pe_ratio,
                        :pb_ratio
                    )
                    ON CONFLICT (ticker, as_of_date) DO UPDATE
                    SET
                        revenue    = EXCLUDED.revenue,
                        earnings   = EXCLUDED.earnings,
                        book_value = EXCLUDED.book_value,
                        eps        = EXCLUDED.eps,
                        pe_ratio   = EXCLUDED.pe_ratio,
                        pb_ratio   = EXCLUDED.pb_ratio
                    """
                ),
                {
                    "ticker": t,
                    "as_of_date": today,
                    "revenue": 1_000_000.0,
                    "earnings": 100_000.0,
                    "book_value": 500_000.0,
                    "eps": 5.0,
                    "pe_ratio": 20.0,
                    "pb_ratio": 2.0,
                },
            )