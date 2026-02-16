import pandas as pd
from dashboard.dashboard_data import STRAT_RET_PARQUET

df = pd.read_parquet(STRAT_RET_PARQUET)
print("Columns:", df.columns.tolist())
print(df.head())