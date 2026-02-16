import pandas as pd
from pathlib import Path
from dashboard_app import compute_attribution_production

def test_event_to_daily_sum(tmp_path, monkeypatch):
    analytics = tmp_path / "analytics"
    analytics.mkdir()
    df = pd.DataFrame([
        {"date":"2021-01-01","ticker":"AAA","realized_return":0.01},
        {"date":"2021-01-01","ticker":"AAA","realized_return":0.02},
        {"date":"2021-01-02","ticker":"AAA","realized_return":0.03},
    ])
    weights = pd.DataFrame([{"date":"2021-01-01","ticker":"AAA","weight_trading_v2":0.1},{"date":"2021-01-02","ticker":"AAA","weight_trading_v2":0.1}])
    df.to_parquet(analytics / "realized_returns.parquet", index=False)
    weights.to_csv(analytics / "quant_portfolio_weights_ensemble_risk_longshort_v2_trading.csv", index=False)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    daily, top, merged, diag = compute_attribution_production(weight_col_choice="weight_trading_v2", normalize_weights_flag=False, calendar_freq="D", rebuild_weights=True)
    assert not merged.empty
    row = merged[(merged['date'] == pd.to_datetime("2021-01-01")) & (merged['_tk'] == "AAA")]
    assert abs(row['realized_return'].iloc[0] - 0.03) < 1e-12