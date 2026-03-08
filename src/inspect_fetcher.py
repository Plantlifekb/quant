import pandas as pd
import importlib
mod = importlib.import_module('quant.ingestion_5years_quant_v1')
for fn in ('run','fetch_all','ingest','fetch_prices','get_prices'):
    if hasattr(mod, fn):
        df = getattr(mod, fn)()
        print('used', fn)
        break
else:
    df = getattr(mod, 'DATAFRAME', None)

print('type(df):', type(df))
if df is None:
    print('NO DATAFRAME RETURNED')
else:
    print('shape:', df.shape)
    print('dtypes:')
    print(df.dtypes)
    print('head:')
    print(df.head(5).to_string(index=False))
    try:
        print('max raw Date:', df['Date'].max())
        print('pd.to_datetime max:', pd.to_datetime(df['Date'], errors='coerce').max())
    except Exception as e:
        print('Date inspect error:', e)
