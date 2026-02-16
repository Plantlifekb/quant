import pandas as pd
import numpy as np

# ============================================================
# CONFIG
# ============================================================
WEEKLY_SELECTION_PATH = r"C:\Quant\data\signals\weekly_selection_longshort_canonical.csv"
PRICES_PATH           = r"C:\Quant\data\ingestion\prices.parquet"

# ============================================================
# 1. LOAD DATA
# ============================================================
print("Loading weekly selections...")
df = pd.read_csv(WEEKLY_SELECTION_PATH)
df['date'] = pd.to_datetime(df['date'])
df['ticker'] = df['ticker'].str.upper().str.strip()

print("Loading prices...")
prices = pd.read_parquet(PRICES_PATH)
prices['date'] = pd.to_datetime(prices['date'])
prices['ticker'] = prices['ticker'].str.upper().str.strip()

# ============================================================
# 2. BUILD WEEKLY RETURNS FROM PRICES
# ============================================================
print("Building weekly returns from prices...")

prices['week_start'] = (
    prices['date']
    .dt.to_period("W-MON")
    .apply(lambda r: r.start_time.normalize())
)

weekly_prices = (
    prices.sort_values(['ticker', 'date'])
          .groupby(['ticker', 'week_start'])['price']
          .last()
          .reset_index()
)

weekly_prices['ret'] = (
    weekly_prices.groupby('ticker')['price'].pct_change()
)

# ============================================================
# 3. MERGE WEEKLY RETURNS INTO WEEKLY SELECTIONS
# ============================================================
print("Merging weekly returns into selections...")

df_perf = df.merge(
    weekly_prices[['ticker', 'week_start', 'ret']],
    left_on=['date', 'ticker'],
    right_on=['week_start', 'ticker'],
    how='left'
)

# basic sanity
missing_ret = df_perf['ret'].isna().mean()
print(f"Fraction of rows with missing returns: {missing_ret:.4%}")

# ============================================================
# 4. LONG / SHORT CONTRIBUTIONS & PORTFOLIO RETURNS
# ============================================================
print("Computing long/short contributions and portfolio returns...")

df_perf['signed_weight'] = np.where(
    df_perf['side'] == 'long',
    df_perf['weight'],
    -df_perf['weight']
)

df_perf['contribution'] = df_perf['signed_weight'] * df_perf['ret']

weekly_portfolio = (
    df_perf.groupby('date')['contribution']
           .sum()
           .reset_index()
           .rename(columns={'contribution': 'weekly_return'})
)

weekly_portfolio['cum_return'] = (1 + weekly_portfolio['weekly_return']).cumprod()

# ============================================================
# 5. LONG VS SHORT BEHAVIOUR
# ============================================================
print("Computing long vs short behaviour...")

long_weekly = (
    df_perf[df_perf['side'] == 'long']
    .groupby('date')['contribution']
    .sum()
    .reset_index()
    .rename(columns={'contribution': 'long_return'})
)

short_weekly = (
    df_perf[df_perf['side'] == 'short']
    .groupby('date')['contribution']
    .sum()
    .reset_index()
    .rename(columns={'contribution': 'short_return'})
)

ls = weekly_portfolio.merge(long_weekly, on='date', how='left') \
                     .merge(short_weekly, on='date', how='left')

ls['long_cum'] = (1 + ls['long_return']).cumprod()
ls['short_cum'] = (1 + ls['short_return']).cumprod()

# ============================================================
# 6. TURNOVER
# ============================================================
print("Computing turnover...")

# absolute weight change per ticker per week, summed across tickers
df_sorted = df.sort_values(['ticker', 'date'])

df_sorted['prev_date'] = df_sorted.groupby('ticker')['date'].shift(1)
df_sorted['prev_weight'] = df_sorted.groupby('ticker')['weight'].shift(1)
df_sorted['prev_side'] = df_sorted.groupby('ticker')['side'].shift(1)

# if side flips, treat previous weight as 0 for turnover (full switch)
side_changed = df_sorted['side'] != df_sorted['prev_side']
df_sorted.loc[side_changed, 'prev_weight'] = 0.0

df_sorted['abs_change'] = (df_sorted['weight'] - df_sorted['prev_weight']).abs()

turnover_weekly = (
    df_sorted.groupby('date')['abs_change']
             .sum()
             .reset_index()
             .rename(columns={'abs_change': 'turnover'})
)

# drop first week (no prior)
first_date = df_sorted['date'].min()
turnover_weekly = turnover_weekly[turnover_weekly['date'] > first_date]

avg_turnover = turnover_weekly['turnover'].mean()

# ============================================================
# 7. RETURN DISTRIBUTION & ROLLING METRICS
# ============================================================
print("Computing return distribution and rolling metrics...")

wr = weekly_portfolio['weekly_return']

sharpe = (wr.mean() / wr.std()) * np.sqrt(52)

cum = weekly_portfolio['cum_return']
roll_max = cum.cummax()
drawdown = (cum - roll_max) / roll_max
max_dd = drawdown.min()

# rolling Sharpe (26-week window)
window = 26
rolling_sharpe = (
    wr.rolling(window)
      .apply(lambda x: (x.mean() / x.std()) * np.sqrt(52) if x.std() != 0 else np.nan)
)

weekly_portfolio['rolling_sharpe_26w'] = rolling_sharpe
weekly_portfolio['drawdown'] = drawdown

# ============================================================
# 8. TOP CONTRIBUTORS & CONCENTRATION
# ============================================================
print("Computing top contributors and concentration...")

# total contribution per ticker
ticker_contrib = (
    df_perf.groupby('ticker')['contribution']
           .sum()
           .reset_index()
           .sort_values('contribution', ascending=False)
)

top10 = ticker_contrib.head(10)

# average weekly Herfindahl index (weight concentration) for long book
long_weights = df[df['side'] == 'long'].copy()
long_weights['w2'] = long_weights['weight'] ** 2

herfindahl = (
    long_weights.groupby('date')['w2']
                .sum()
                .reset_index()
                .rename(columns={'w2': 'herfindahl_long'})
)

avg_herfindahl = herfindahl['herfindahl_long'].mean()

# ============================================================
# 9. SUMMARY OUTPUT
# ============================================================
print("\n===== PERFORMANCE ANATOMY SUMMARY =====")
print(f"Sharpe Ratio (weekly):        {sharpe:.4f}")
print(f"Max Drawdown:                 {max_dd:.4%}")
print(f"Final Cumulative Return:      {weekly_portfolio['cum_return'].iloc[-1]:.4f}")
print(f"Average Weekly Turnover:      {avg_turnover:.4f}")
print(f"Average Long Herfindahl:      {avg_herfindahl:.4f}")
print("=======================================\n")

print("First 5 rows of long/short cumulative returns:")
print(ls[['date', 'long_return', 'short_return', 'long_cum', 'short_cum']].head(), "\n")

print("First 5 rows of turnover:")
print(turnover_weekly.head(), "\n")

print("Top 10 tickers by total contribution:")
print(top10, "\n")

print("First 5 rows of weekly portfolio with rolling Sharpe and drawdown:")
print(weekly_portfolio[['date', 'weekly_return', 'cum_return', 'rolling_sharpe_26w', 'drawdown']].head())