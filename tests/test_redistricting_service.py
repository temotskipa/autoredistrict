from pathlib import Path

import pandas as pd

from src.services import redistricting_service
from src.services.redistricting_service import DemoPlanRequest, normalize_state_fips, run_demo_plan


def test_run_demo_plan_writes_artifacts(tmp_path: Path):
    result = run_demo_plan(
        DemoPlanRequest(districts=4, algorithm="fair", grid_size=4),
        artifact_root=tmp_path,
    )

    assert result["summary"]["requested_districts"] == 4
    assert result["summary"]["district_count"] == 4
    assert result["summary"]["all_contiguous"] is True
    assert result["summary"]["max_abs_deviation_pct"] <= 2.0
    assert result["solver"]["solver"] == "graph_balanced_local_search"
    assert result["solver"]["random_seed"] == 0
    assert result["solver"]["recombination_passes"] == 4
    assert "adjacent_pair_recom_rebalancing" in result["solver"]["video_informed_features"]
    assert len(result["assignment"]) == 16
    assert len(result["metrics"]) == 4
    assert Path(result["artifacts"]["map_png"]).exists()
    assert Path(result["artifacts"]["districts_geojson"]).exists()
    assert Path(result["artifacts"]["assignment_csv"]).exists()
    assert Path(result["artifacts"]["metrics_csv"]).exists()
    assert Path(result["artifacts"]["report_json"]).exists()

    assignment = pd.read_csv(result["artifacts"]["assignment_csv"], dtype=str)
    assert set(assignment.columns) == {"GEOID", "district_id"}
    assert len(assignment) == 16


def test_run_demo_plan_records_vra_request(tmp_path: Path):
    result = run_demo_plan(
        DemoPlanRequest(districts=4, algorithm="fair", grid_size=4, vra=True),
        artifact_root=tmp_path,
    )

    assert result["source"]["vra_requested"] is True
    assert result["solver"]["vra_compliance_requested"] is True
    assert result["solver"]["statewide_minority_share"] > 0.3


def test_run_demo_plan_preserves_demo_coi(tmp_path: Path):
    result = run_demo_plan(
        DemoPlanRequest(districts=4, algorithm="fair", grid_size=4, preserve_demo_coi=True),
        artifact_root=tmp_path,
    )

    district_ids = {result["assignment"][geoid] for geoid in ["0000000", "0000001", "0000002"]}

    assert len(district_ids) == 1
    assert result["solver"]["communities_of_interest_units"] == 3
    assert result["summary"]["all_contiguous"] is True


def test_normalize_state_fips_accepts_name_abbr_and_fips():
    assert normalize_state_fips("ME") == "23"
    assert normalize_state_fips("Maine") == "23"
    assert normalize_state_fips("23") == "23"


def test_run_state_plan_uses_fetcher_and_writes_artifacts(tmp_path: Path, monkeypatch):
    demo = redistricting_service.build_demo_dataset(size=4)
    census_df = demo.drop(columns="geometry").copy()
    shape_path = tmp_path / "units.geojson"
    demo[["GEOID", "geometry"]].to_file(shape_path, driver="GeoJSON")

    class FakeDataFetcherWorker:
        def __init__(
            self,
            state_fips,
            api_key,
            election_year=None,
            provider_keys=None,
            resolution="tract",
        ):
            assert state_fips == "23"
            assert api_key == "test-key"
            assert election_year == 2024
            assert provider_keys == ["county_presidential"]
            assert resolution == "tract"

        def _get_census_data(self, state_fips):
            assert state_fips == "23"
            return census_df

        def _get_shapefiles(self, state_fips):
            assert state_fips == "23"
            return str(shape_path)

    monkeypatch.setattr(
        redistricting_service,
        "DataFetcherWorker",
        FakeDataFetcherWorker,
    )

    result = redistricting_service.run_state_plan(
        redistricting_service.StatePlanRequest(
            state="ME",
            api_key="test-key",
            districts=4,
            election_year=2024,
            provider_keys=["county_presidential"],
            resolution="tract",
            random_seed=9,
        ),
        artifact_root=tmp_path / "plans",
    )

    assert result["source"]["mode"] == "state"
    assert result["source"]["state_fips"] == "23"
    assert result["source"]["resolution"] == "tract"
    assert result["solver"]["random_seed"] == 9
    assert result["summary"]["district_count"] == 4
    assert result["summary"]["all_contiguous"] is True
    assert Path(result["artifacts"]["report_json"]).exists()
