"""Tests for importing Redistricting Data Hub downloads into the Census cache.

The fixtures mimic RDH's 2020 PL 94-171 files: a ``xx_pl2020_t.zip`` holding a
shapefile keyed by GEOID20 with legacy count columns (P0010001 and so on), or
the CSV version of the same table.
"""

import json
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from src.cli import main
from src.data import census_fetcher
from src.data.census_fetcher import DataFetcherWorker
from src.data.rdh_import import RdhImportError, import_rdh_files
from src.services.redistricting_service import merge_census_geodata


def _rdh_tracts(state="23", county="001", size=4) -> gpd.GeoDataFrame:
    rows = []
    for row in range(size):
        for col in range(size):
            index = row * size + col
            total = 1000 + 10 * index
            rows.append(
                {
                    "GEOID20": f"{state}{county}{index + 1:04d}00",
                    "NAME": f"Census Tract {index + 1}",
                    "P0010001": total,
                    "P0010002": total - 40,
                    "P0010003": total - 400,
                    "P0010004": 150,
                    "P0010005": 60,
                    "P0010006": 80,
                    "P0010007": 20,
                    "P0010008": 50,
                    "geometry": box(-70 + col * 0.1, 44 + row * 0.1,
                                    -70 + (col + 1) * 0.1, 44 + (row + 1) * 0.1),
                }
            )
    return gpd.GeoDataFrame(rows, crs="EPSG:4269")


def _zip_shapefile(gdf: gpd.GeoDataFrame, tmp_path: Path, name: str) -> Path:
    folder = tmp_path / "download" / name
    folder.mkdir(parents=True)
    gdf.to_file(folder / f"{name}.shp")
    (folder / f"readme_{name}.txt").write_text("RDH readme", encoding="utf-8")
    zip_path = tmp_path / f"{name}.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        for file in folder.iterdir():
            archive.write(file, file.name)
    return zip_path


@pytest.fixture
def offline(monkeypatch):
    """Fail on any census.gov request and skip partisan downloads."""

    def no_network(*_args, **_kwargs):
        raise AssertionError("census.gov should not be contacted for imported data")

    monkeypatch.setattr(census_fetcher.requests, "head", no_network)
    monkeypatch.setattr(census_fetcher.requests, "get", no_network)
    monkeypatch.setattr(census_fetcher, "fetch_scores_for_provider", lambda *_: None)
    monkeypatch.delenv("CENSUS_API_KEY", raising=False)


def test_import_shapefile_zip_fills_cache_for_keyless_runs(tmp_path, monkeypatch, offline):
    monkeypatch.chdir(tmp_path)
    tracts = _rdh_tracts()
    zip_path = _zip_shapefile(tracts, tmp_path, "me_pl2020_t")

    [result] = import_rdh_files([zip_path])

    assert (result.state_fips, result.resolution, result.rows) == ("23", "tract", 16)
    assert result.total_population == int(tracts["P0010001"].sum())
    manifest = json.loads(
        (tmp_path / ".cache" / "shapefiles_23_tract" / "source.json").read_text(encoding="utf-8")
    )
    assert manifest["input_files"] == ["me_pl2020_t.shp"]

    worker = DataFetcherWorker("23", None, resolution="tract")
    census_df = worker._get_census_data("23")
    geometry_path = worker._get_shapefiles("23")
    merged = merge_census_geodata(geometry_path, census_df)

    assert worker._census is None
    assert len(merged) == 16
    assert merged["P1_001N"].sum() == result.total_population
    assert merged["P1_004N"].eq(150).all()
    assert merged["tract"].iloc[0] == "000100"


def test_import_csv_with_numeric_geoids_and_separate_geometry(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    tracts = _rdh_tracts(state="01")
    counts = pd.DataFrame(tracts.drop(columns="geometry"))
    counts["GEOID20"] = counts["GEOID20"].astype("int64")  # leading zero lost
    counts.to_csv(tmp_path / "al_pl2020_t.csv", index=False)
    boundaries = tmp_path / "al_t_2020_bound"
    boundaries.mkdir()
    tracts[["GEOID20", "geometry"]].to_file(boundaries / "al_t_2020_bound.shp")

    [result] = import_rdh_files([tmp_path / "al_pl2020_t.csv", boundaries])

    cached = pd.read_csv(result.census_csv, dtype={"GEOID": str})
    assert result.state_fips == "01"
    assert cached["GEOID"].str.startswith("01001").all()
    assert cached["GEOID"].str.len().eq(11).all()


def test_import_rejects_files_for_another_state(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    zip_path = _zip_shapefile(_rdh_tracts(), tmp_path, "me_pl2020_t")

    with pytest.raises(RdhImportError, match="state FIPS 23, not 50"):
        import_rdh_files([zip_path], state_fips="50")


def test_import_needs_geometry(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    counts = pd.DataFrame(_rdh_tracts().drop(columns="geometry"))
    counts.to_csv(tmp_path / "me_pl2020_t.csv", index=False)

    with pytest.raises(RdhImportError, match="No geometry"):
        import_rdh_files([tmp_path / "me_pl2020_t.csv"])

    assert not (tmp_path / ".cache" / "census_23_tract.csv").exists()


def test_cli_imports_then_runs_live_without_key(tmp_path, monkeypatch, capsys, offline):
    monkeypatch.chdir(tmp_path)
    zip_path = _zip_shapefile(_rdh_tracts(), tmp_path, "me_pl2020_t")
    report_path = tmp_path / "plan.json"

    main(["ME", "--import-rdh", str(zip_path), "--quiet"])
    assert "Imported state 23 at tract level: 16 units" in capsys.readouterr().out

    main(
        [
            "ME",
            "--mode",
            "live",
            "--resolution",
            "tract",
            "--districts",
            "2",
            "--map-out",
            str(tmp_path / "map.png"),
            "--report-out",
            str(report_path),
            "--quiet",
        ]
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["summary"]["district_count"] == 2
    assert report["source"]["mode"] == "live"
    assert report["source"]["shapefile_path"].endswith("geometry.gpkg")
