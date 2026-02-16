import pandas as pd
from pathlib import Path
from recompute_perf_safe import compute_performance_safe

def test_perf_parity(tmp_path, monkeypatch):
    analytics = tmp_path / "analytics"
    analytics.mkdir()
    merged = pd.DataFrame([
        {"date":"2021-01-01","_tk":"AAA","_weight_used":0.1,"realized_return":0.01,"contrib_total":0.001},
        {"date":"2021-01-02","_tk":"AAA","_weight_used":0.1,"realized_return":0.02,"contrib_total":0.002},
    ])
    merged.to_parquet(analytics / "merged_attribution.parquet", index=False)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    perf = compute_performance_safe(str(analytics / "merged_attribution.parquet"), str(analytics / "perf_weekly.parquet"), freq="W", persist=False)
    assert not perf.empty
    assert abs(perf['period_return'].sum() - 0.003) < 1e-12