import json
from pathlib import Path

from src import cli
from src.cli import main
from src.services.redistricting_service import build_demo_dataset


def test_cli_demo_writes_map_and_report_with_seed(tmp_path: Path):
    map_path = tmp_path / "demo.png"
    report_path = tmp_path / "plan.json"

    main(
        [
            "demo",
            "--demo",
            "--mode",
            "demo",
            "--districts",
            "4",
            "--seed",
            "11",
            "--map-out",
            str(map_path),
            "--report-out",
            str(report_path),
            "--quiet",
        ]
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))

    assert map_path.exists()
    assert report["solver"]["random_seed"] == 11
    assert report["summary"]["district_count"] == 4
    assert report["summary"]["all_contiguous"] is True


def test_cli_demo_preserves_coi_from_csv(tmp_path: Path):
    coi_path = tmp_path / "coi.csv"
    coi_path.write_text("GEOID\n0000000\n0000001\n0000002\n", encoding="utf-8")
    report_path = tmp_path / "plan.json"

    main(
        [
            "demo",
            "--mode",
            "demo",
            "--districts",
            "4",
            "--coi-csv",
            str(coi_path),
            "--map-out",
            str(tmp_path / "coi.png"),
            "--report-out",
            str(report_path),
            "--quiet",
        ]
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    coi_districts = {report["assignment"][geoid] for geoid in ("0000000", "0000001", "0000002")}

    assert report["solver"]["communities_of_interest_units"] == 3
    assert len(coi_districts) == 1


def test_cli_smoke_without_key_or_cache_falls_back_to_demo(tmp_path: Path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)  # no .cache or config.json here
    monkeypatch.delenv("CENSUS_API_KEY", raising=False)
    report_path = tmp_path / "smoke.json"

    main(
        [
            "--mode",
            "smoke",
            "--map-out",
            str(tmp_path / "smoke.png"),
            "--report-out",
            str(report_path),
            "--quiet",
        ]
    )

    out = capsys.readouterr().out
    report = json.loads(report_path.read_text(encoding="utf-8"))

    assert "using demo dataset" in out
    assert "Smoke test passed" in out
    assert report["summary"]["district_count"] == 4


def test_cli_live_fallback_uses_official_apportionment_table(tmp_path: Path, monkeypatch):
    demo = build_demo_dataset(size=4)
    demo["P1_001N"] = 68_000  # ~1.088M total (Montana), which rounds to 1 under pop / 760k
    census_df = demo.drop(columns="geometry").copy()
    shape_path = tmp_path / "mt.geojson"
    demo[["GEOID", "geometry"]].to_file(shape_path, driver="GeoJSON")
    fake_map = tmp_path / "artifact_map.png"
    fake_map.write_bytes(b"png")
    fake_report = tmp_path / "artifact_plan.json"
    fake_report.write_text("{}", encoding="utf-8")

    class FakeWorker:
        def __init__(self, *args, **kwargs):
            pass

        def _get_census_data(self, state_fips):
            return census_df

        def _get_shapefiles(self, state_fips):
            return str(shape_path)

    class FailingDataFetcher:
        def __init__(self, api_key):
            pass

        def get_all_states_population_data(self):
            raise RuntimeError("Census API unreachable")

    captured = {}

    def fake_solve_geodata_plan(**kwargs):
        captured["district_count"] = kwargs["district_count"]
        return {
            "metrics": [],
            "summary": {
                "district_count": kwargs["district_count"],
                "max_abs_deviation_pct": 0.0,
                "average_compactness": 1.0,
                "all_contiguous": True,
            },
            "artifacts": {
                "map_png": str(fake_map),
                "report_json": str(fake_report),
            },
        }

    monkeypatch.setattr(cli, "has_cached_census_data", lambda *args, **kwargs: True)
    monkeypatch.setattr(cli, "DataFetcherWorker", FakeWorker)
    monkeypatch.setattr(cli, "DataFetcher", FailingDataFetcher)
    monkeypatch.setattr(cli, "solve_geodata_plan", fake_solve_geodata_plan)

    main(
        [
            "MT",
            "--mode",
            "live",
            "--resolution",
            "tract",
            "--map-out",
            str(tmp_path / "mt.png"),
            "--quiet",
        ]
    )
    assert captured["district_count"] == 2

    # Explicit --districts still wins over the table.
    main(
        [
            "MT",
            "--mode",
            "live",
            "--resolution",
            "tract",
            "--districts",
            "3",
            "--map-out",
            str(tmp_path / "mt3.png"),
            "--quiet",
        ]
    )
    assert captured["district_count"] == 3
