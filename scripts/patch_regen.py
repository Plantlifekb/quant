from pathlib import Path
p = Path(r"C:\Quant\scripts\regen_and_verify.py")
s = p.read_text(encoding="utf8")
# replace price_col detection with safe adj_close fallback
s = s.replace(
    "price_col = next((c for c in df.columns if c in (\"adj_close\", \"adjclose\", \"close\", \"close_price\", \"price\", \"last\")), None)",
    "price_col = 'adj_close' if 'adj_close' in df.columns and df['adj_close'].notna().any() else ('close' if 'close' in df.columns and df['close'].notna().any() else None)"
)
# replace date parsing to force dayfirst=True
s = s.replace(
    "df[\"date\"] = pd.to_datetime(df[\"date\"], errors=\"coerce\")",
    "df['date'] = pd.to_datetime(df['date'], errors='coerce', dayfirst=True)"
)
p.write_text(s, encoding="utf8")
print('patched', p)
