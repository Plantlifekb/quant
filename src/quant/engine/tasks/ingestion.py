import os
import logging
import importlib
import importlib.util
from typing import Any, Optional, Dict
import pandas as pd

logger = logging.getLogger('quant.engine.tasks.ingestion')
logger.addHandler(logging.NullHandler())

def _import_fetcher_module() -> Optional[Any]:
    candidates = [
        'quant.ingestion_5years_quant_v1',
        'quant.ingestion',
        'quant.engine.tasks.ingestion_fetcher',
        'ingestion_5years_quant_v1',
    ]
    for name in candidates:
        try:
            mod = importlib.import_module(name)
            logger.info('Loaded fetcher module: %s', name)
            return mod
        except Exception:
            continue
    path = os.getenv('INGESTION_FETCHER_PATH')
    if path and os.path.exists(path):
        try:
            spec = importlib.util.spec_from_file_location('quant_ingestion_fetcher', path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)  # type: ignore
            logger.info('Loaded fetcher module from path: %s', path)
            return mod
        except Exception as exc:
            logger.exception('Failed to load fetcher from path %s: %s', path, exc)
    logger.error('No ingestion fetcher module found. Tried: %s', candidates)
    return None

def write_prices_to_db(df: pd.DataFrame) -> int:
    pg_dsn = os.getenv('DATABASE_URL') or os.getenv('PG_DSN') or os.getenv('PGCONN')
    if not pg_dsn:
        host = os.getenv('PGHOST', 'localhost')
        port = os.getenv('PGPORT', '5432')
        dbname = os.getenv('PGDATABASE', os.getenv('PG_DB', 'quant'))
        user = os.getenv('PGUSER', 'quant')
        password = os.getenv('PGPASSWORD', os.getenv('PG_PASS', 'quant'))
        pg_dsn = f'host={host} port={port} dbname={dbname} user={user} password={password}'

    if isinstance(pg_dsn, str) and (pg_dsn.startswith('postgresql+psycopg2://') or pg_dsn.startswith('postgresql://')):
        from sqlalchemy import create_engine
        engine = create_engine(pg_dsn, pool_pre_ping=True)
        df.to_sql('prices', engine, schema='public', if_exists='append', index=False, method='multi')
        return len(df)

    import psycopg2
    from psycopg2 import sql, extras

    if pg_dsn.startswith('postgres://') or pg_dsn.startswith('postgresql://'):
        from urllib.parse import urlparse, unquote
        url = urlparse(pg_dsn)
        conn_kwargs = {
            'host': url.hostname,
            'port': url.port or 5432,
            'dbname': url.path.lstrip('/'),
            'user': url.username,
            'password': unquote(url.password) if url.password else None,
        }
        conn = psycopg2.connect(**{k: v for k, v in conn_kwargs.items() if v is not None})
    else:
        conn = psycopg2.connect(pg_dsn)

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    '''
                    CREATE TABLE IF NOT EXISTS public.prices (
                        ticker text,
                        date date,
                        adj_close numeric,
                        close numeric,
                        high numeric,
                        low numeric,
                        open numeric,
                        volume bigint
                    );
                    '''
                )
                df_cols = [c for c in df.columns]
                col_map = {
                    'Date': 'date',
                    'date': 'date',
                    'Adj_Close': 'adj_close',
                    'AdjClose': 'adj_close',
                    'Adj Close': 'adj_close',
                    'Close': 'close',
                    'High': 'high',
                    'Low': 'low',
                    'Open': 'open',
                    'Volume': 'volume',
                    'ticker': 'ticker',
                    'TICKER': 'ticker',
                }
                db_cols = [col_map.get(c, c).lower() for c in df_cols]
                cols_sql = sql.SQL(', ').join(sql.Identifier(c) for c in db_cols)
                insert_sql = sql.SQL('INSERT INTO public.prices ({cols}) VALUES %s').format(cols=cols_sql)

                values = [tuple(None if pd.isna(x) else x for x in row) for row in df.itertuples(index=False, name=None)]
                batch_size = 1000
                for i in range(0, len(values), batch_size):
                    batch = values[i:i+batch_size]
                    extras.execute_values(cur, insert_sql.as_string(cur), batch, template=None, page_size=batch_size)
        return len(df)
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _compute_last_date_from_series(s: pd.Series) -> Optional[str]:
    try:
        ts = pd.to_datetime(s, errors='coerce').max()
        if pd.isna(ts):
            return None
        try:
            return ts.date().isoformat()
        except Exception:
            return pd.to_datetime(ts).strftime('%Y-%m-%d')
    except Exception:
        try:
            m = s.max()
            if m is None:
                return None
            return str(m)
        except Exception:
            return None

def task_ingest_and_write(engine=None, *, fetcher_module: Optional[Any] = None, **kwargs) -> Dict[str, Any]:
    try:
        mod = fetcher_module or _import_fetcher_module()
        if mod is None:
            msg = 'No fetcher module available'
            logger.error(msg)
            return {'status': 'fetcher_missing', 'error': msg}

        fetch_fns = ['fetch_all', 'ingest', 'run', 'fetch_prices', 'get_prices']
        df = None
        for fn in fetch_fns:
            if hasattr(mod, fn):
                try:
                    df = getattr(mod, fn)()
                    logger.info('Fetched data using %s.%s', mod.__name__, fn)
                    break
                except TypeError:
                    try:
                        df = getattr(mod, fn)(None)
                        logger.info('Fetched data using %s.%s with None arg', mod.__name__, fn)
                        break
                    except Exception:
                        continue

        # If fetcher returned a DataFrame, normalize common lowercase column names to canonical names
        if df is not None and isinstance(df, pd.DataFrame):
            df.rename(
                columns={
                    'date': 'Date',
                    'adj_close': 'Adj_Close',
                    'adjclose': 'Adj_Close',
                    'adj close': 'Adj_Close',
                    'close': 'Close',
                    'high': 'High',
                    'low': 'Low',
                    'open': 'Open',
                    'volume': 'Volume',
                    'ticker': 'ticker',
                    'ticker_symbol': 'ticker',
                },
                inplace=True,
            )

        if df is None and hasattr(mod, 'DATAFRAME'):
            df = getattr(mod, 'DATAFRAME')

        if df is None:
            msg = 'Fetcher module did not return a DataFrame'
            logger.error(msg)
            return {'status': 'fetch_failed', 'error': msg}

        # Ensure Date column is canonical and converted to plain date objects
        if 'Date' in df.columns and len(df) > 0:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            try:
                if df['Date'].dt.tz is not None:
                    df['Date'] = df['Date'].dt.tz_convert(None)
            except Exception:
                pass
            df['Date'] = df['Date'].dt.date

        expected = {
            'Date': 'Date',
            'Adj_Close': 'Adj_Close',
            'Close': 'Close',
            'High': 'High',
            'Low': 'Low',
            'Open': 'Open',
            'Volume': 'Volume',
            'ticker': 'ticker',
            'Ticker': 'ticker',
        }
        normalized = {}
        for src, dst in expected.items():
            if src in df.columns:
                normalized[dst] = df[src]

        if 'ticker' not in normalized:
            ticker = getattr(mod, 'TICKER', None) or os.getenv('INGESTION_TICKER')
            if ticker:
                normalized['ticker'] = pd.Series([ticker] * len(df))

        final = pd.DataFrame(normalized)

        # Compute last_date robustly from the Date column before DB write
        last_date = None
        if 'Date' in final.columns and len(final) > 0:
            last_date = _compute_last_date_from_series(final['Date'])

        rows = write_prices_to_db(final)

        logger.info('Completed ingestion wrapper: %s', {'status': 'ok', 'rows_written': rows, 'last_date': last_date})
        return {'status': 'ok', 'rows_written': rows, 'last_date': last_date}
    except Exception as exc:
        logger.exception('DB write failed -> %s', exc)
        return {'status': 'db_write_failed', 'error': str(exc)}

def run(*args, **kwargs):
    return task_ingest_and_write(*args, **kwargs)

if __name__ == '__main__':
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO)
    print(task_ingest_and_write())


