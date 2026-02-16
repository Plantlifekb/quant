import pandas as pd, os, sys

src = r'C:\Quant\scripts\validation\df_perf_export.csv'
dst = r'C:\Quant\scripts\validation\df_perf_export.csv'  # overwrite in place with normalized schema

if not os.path.exists(src):
    print('ERROR: source CSV not found:', src)
    sys.exit(1)

df = pd.read_csv(src)
print('Original columns:', list(df.columns))

# rename as_of_date -> date if present
if 'as_of_date' in df.columns:
    df = df.rename(columns={'as_of_date':'date'})

# ensure date column exists and is parseable
if 'date' not in df.columns:
    print('ERROR: no date column found after rename. Columns:', list(df.columns))
    sys.exit(2)

df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.normalize()
if df['date'].isna().all():
    sample = df.iloc[:5].to_dict(orient='records')
    print('ERROR: date column could not be parsed. Sample values:', sample)
    sys.exit(3)

# ensure ticker exists
if 'ticker' not in df.columns:
    print('ERROR: ticker column not found. Columns:', list(df.columns))
    sys.exit(4)
df['ticker'] = df['ticker'].astype(str).str.upper().str.strip()

# keep only relevant columns and add defaults
out = df[['date','ticker']].copy()
# preserve weight if present
if 'weight' in df.columns:
    out['weight'] = pd.to_numeric(df['weight'], errors='coerce')
# preserve side if present, otherwise default to long
if 'side' in df.columns:
    out['side'] = df['side'].astype(str).str.lower()
else:
    out['side'] = 'long'

# optional score column if present
if 'score' in df.columns:
    out['score'] = df['score']

out.to_csv(dst, index=False)
print('Wrote normalized CSV:', dst)
print('Output columns:', list(out.columns))
print('Rows:', len(out))
