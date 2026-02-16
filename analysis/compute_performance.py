import pandas as pd
import numpy as np

# ------------------------------------------------------------
# Load weekly selections
# ------------------------------------------------------------
df = pd.read_csv(r"C:\Quant\data\signals\weekly_selection_longshort_canonical.csv")
df['date'] = pd.to_datetime(df['date'])
df['ticker'] = df['ticker'].str.upper().str.strip()

# ------------------------------------------------------------
# Load daily prices
# ------------------------------------------------------------
prices = pd.read_parquet(r"C:\Quant\data\ingestion\prices.parquet")
prices['date'] = pd.to_datetime(prices['date'])
prices['ticker'] = prices['ticker'].str.upper().str.strip()

# ------------------------------------------------------------
# Convert daily prices → weekly returns
# ------------------------------------------------------------
prices['week_start'] = (
    prices['date']
    .dt.to_period("W-MON")
    .apply(lambda r: r.start_time.normalize())
)

# last price of each week
weekly_prices = (
    prices.sort_values(['ticker','date'])
          .groupby(['ticker','week_start'])['price']
          .last()
          .reset_index()
)

# compute weekly returns from weekly prices
weekly_prices['ret'] = (
    weekly_prices.groupby('ticker')['price'].pct_change()
)

# ------------------------------------------------------------
# Merge weekly returns into weekly selections
# ------------------------------------------------------------
df_perf = df.merge(
    weekly_prices[['ticker','week_start','ret']],
    left_on=['date','ticker'],
    right_on=['week_start','ticker'],
    how='left'
)

# ------------------------------------------------------------
# Compute long/short contributions
# ------------------------------------------------------------
df_perf['signed_weight'] = np.where(
    df_perf['side']=='long',
    df_perf['weight'],
    -df_perf['weight']
)

df_perf['contribution'] = df_perf['signed_weight'] * df_perf['ret']

# ------------------------------------------------------------
# Aggregate to weekly portfolio returns
# ------------------------------------------------------------
weekly_portfolio = (
    df_perf.groupby('date')['contribution']
           .sum()
           .reset_index()
           .rename(columns={'contribution':'weekly_return'})
)

# ------------------------------------------------------------
# Compute cumulative return
# ------------------------------------------------------------
weekly_portfolio['cum_return'] = (1 + weekly_portfolio['weekly_return']).cumprod()

# ------------------------------------------------------------
# Compute Sharpe and drawdown
# ------------------------------------------------------------
sharpe = (
    weekly_portfolio['weekly_return'].mean() /
    weekly_portfolio['weekly_return'].std()
) * np.sqrt(52)

cum = weekly_portfolio['cum_return']
roll_max = cum.cummax()
drawdown = (cum - roll_max) / roll_max
max_dd = drawdown.min()

# ------------------------------------------------------------
# Print summary
# ------------------------------------------------------------
print("\n===== PERFORMANCE SUMMARY =====")
print("Sharpe Ratio:", sharpe)
print("Max Drawdown:", max_dd)
print("Final Cumulative Return:", weekly_portfolio['cum_return'].iloc[-1])
print("================================\n")

print(weekly_portfolio.head())