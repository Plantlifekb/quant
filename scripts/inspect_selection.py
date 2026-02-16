import pandas as pd

df = pd.read_csv(r"C:\Quant\data\signals\weekly_selection.csv")
print(df.head())
print("\nCOLUMNS:")
print(df.columns.tolist())