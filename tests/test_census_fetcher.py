from pathlib import Path

import pandas as pd

from src.data.census_fetcher import DataFetcherWorker


def test_cached_census_data_loads_without_api_key(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cache_dir = tmp_path / ".cache"
    cache_dir.mkdir()
    pd.DataFrame(
        {
            "GEOID": ["23001010100"],
            "county": ["001"],
            "P1_001N": [4000],
            "partisan_score": [0.55],
        }
    ).to_csv(cache_dir / "census_23_tract.csv", index=False)

    worker = DataFetcherWorker("23", None, resolution="tract")
    census_df = worker._get_census_data("23")

    assert census_df["GEOID"].tolist() == ["23001010100"]
    assert worker._census is None  # the Census client was never needed
