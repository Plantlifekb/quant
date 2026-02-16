import pandas as pd, os, sys
src = r'C:\Quant\scripts\validation\df_perf_export.parquet'
dst = r'C:\Quant\scripts\validation\df_perf_export.csv'
if not os.path.exists(src):
    print('ERROR: parquet not found:', src); sys.exit(1)
df = pd.read_parquet(src)
df.to_csv(dst, index=False)
print('Wrote CSV:', dst)
