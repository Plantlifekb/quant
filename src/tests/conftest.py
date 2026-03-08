import os
from pathlib import Path
import pytest

@pytest.fixture(autouse=True)
def ensure_ticker_reference(tmp_path, monkeypatch):
    repo_fixture = Path(__file__).parent / "fixtures" / "ticker_reference.csv"
    if repo_fixture.exists():
        monkeypatch.setenv("INGESTION_TICKER_REF", str(repo_fixture))
    else:
        cfg = tmp_path / "ticker_reference.csv"
        cfg.write_text(
            "ticker,company_name,market_sector\n"
            "AAPL,Apple Inc.,Technology\n"
            "MSFT,Microsoft Corp.,Technology\n"
            "ZTS,Zooey Therapeutics,Healthcare\n",
            encoding="utf8",
        )
        monkeypatch.setenv("INGESTION_TICKER_REF", str(cfg))
