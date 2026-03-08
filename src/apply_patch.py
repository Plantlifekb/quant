import io,sys,re
p = 'quant/engine/tasks/ingestion.py'
with open(p, 'r', encoding='utf8') as f:
    s = f.read()

# Find the fetch loop end marker: the first occurrence of "for fn in fetch_fns:"
m = re.search(r'(for\s+fn\s+in\s+fetch_fns:.*?break\s*)', s, flags=re.S)
if not m:
    print('ERROR: fetch loop marker not found', file=sys.stderr)
    sys.exit(1)

insert_after = m.end()
# Build the normalization block with correct indentation (8 spaces inside function)
block = (
    "\n        # --- Normalize incoming fetcher DataFrame column names to canonical names ---\n"
    "        if df is not None and isinstance(df, pd.DataFrame):\n"
    "            df.rename(\n"
    "                columns={\n"
    "                    'date': 'Date',\n"
    "                    'adj_close': 'Adj_Close',\n"
    "                    'adjclose': 'Adj_Close',\n"
    "                    'adj close': 'Adj_Close',\n"
    "                    'close': 'Close',\n"
    "                    'high': 'High',\n"
    "                    'low': 'Low',\n"
    "                    'open': 'Open',\n"
    "                    'volume': 'Volume',\n"
    "                    'ticker': 'ticker',\n"
    "                    'ticker_symbol': 'ticker',\n"
    "                },\n"
    "                inplace=True,\n"
    "            )\n\n"
)

# Only insert if not already present
if 'Normalize incoming fetcher DataFrame column names' not in s:
    s = s[:insert_after] + block + s[insert_after:]
    with open(p, 'w', encoding='utf8') as f:
        f.write(s)
    print('PATCH APPLIED')
else:
    print('PATCH ALREADY PRESENT')
