import csv
import os
import psycopg2
from datetime import datetime

CSV_PATH = "/data/ingestion/ingestion_5years.csv"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "quantdb"),
    "user": os.getenv("DB_USER", "quant"),
    "password": os.getenv("DB_PASSWORD", "quant_password"),
}

DATE_FORMAT = "%Y-%m-%d"


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def parse_date(date_str: str) -> datetime.date:
    return datetime.strptime(date_str, DATE_FORMAT).date()


def upsert_asset(cur, ticker, company_name, market_sector):
    cur.execute(
        """
        INSERT INTO assets (ticker, company_name, market_sector)
        VALUES (%s, %s, %s)
        ON CONFLICT (ticker)
        DO UPDATE SET company_name = EXCLUDED.company_name,
                      market_sector = EXCLUDED.market_sector
        RETURNING asset_id;
        """,
        (ticker, company_name, market_sector),
    )
    return cur.fetchone()[0]


def insert_price(cur, asset_id, row, run_dt):
    cur.execute(
        """
        INSERT INTO prices (
            asset_id, date, open, high, low, close, adj_close, volume, run_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (asset_id, date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            adj_close = EXCLUDED.adj_close,
            volume = EXCLUDED.volume,
            run_date = EXCLUDED.run_date;
        """,
        (
            asset_id,
            parse_date(row["date"]),
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            float(row["adj_close"]),
            int(float(row["volume"])),   # <-- THIS COMMA IS THE FIX
            run_dt,
        ),
    )


def main():
    run_dt = datetime.utcnow()

    with (
        get_connection() as conn,
        conn.cursor() as cur,
        open(CSV_PATH, newline="", encoding="utf-8") as f
    ):
        # FIXED: comma-delimited CSV
        reader = csv.DictReader(f, delimiter=",")

        row_count = 0
        for row in reader:
            asset_id = upsert_asset(
                cur,
                row["ticker"],
                row["company_name"],
                row["market_sector"],
            )
            insert_price(cur, asset_id, row, run_dt)
            row_count += 1

        cur.execute(
            """
            INSERT INTO ingestion_runs (run_date, source_file, row_count)
            VALUES (%s, %s, %s);
            """,
            (run_dt, CSV_PATH, row_count),
        )

    print(f"Ingestion complete. Rows processed: {row_count}")


if __name__ == "__main__":
    main()