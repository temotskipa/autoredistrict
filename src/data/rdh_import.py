"""Import Redistricting Data Hub (RDH) downloads into the local Census cache.

census.gov sits behind a Cloudflare rule that blocks some regions, which stops
both the Census API and the TIGER shapefile downloads. RDH republishes the same
2020 P.L. 94-171 counts joined to Census geometry, for example
``me_pl2020_t.zip`` (tracts) and ``me_pl2020_b.zip`` (blocks). This module
turns those files into the cache layout ``DataFetcherWorker`` reads, so the CLI
and web app can run for an imported state without census.gov or an API key.

Accepted inputs are zips, directories, shapefiles and CSVs. Counts and
geometry may come from one file (RDH's shapefile download) or from two (RDH's
CSV download plus a boundary shapefile).
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import geopandas as gpd
import pandas as pd

from .census_fetcher import (
    CACHE_DIR,
    IMPORT_MANIFEST,
    census_cache_paths,
    record_cache_index,
    shapefile_cache_dir,
)

logger = logging.getLogger(__name__)

# RDH ships the legacy P.L. 94-171 field names; the cache uses the Census API
# names that DataFetcherWorker requests.
LEGACY_TO_API = {
    "P0010001": "P1_001N",  # total population
    "P0010003": "P1_003N",  # White alone
    "P0010004": "P1_004N",  # Black alone
    "P0010005": "P1_005N",  # American Indian and Alaska Native alone
    "P0010006": "P1_006N",  # Asian alone
    "P0010007": "P1_007N",  # Native Hawaiian and Other Pacific Islander alone
    "P0010008": "P1_008N",  # Some Other Race alone
}
COUNT_FIELDS = tuple(LEGACY_TO_API.values())
GEOID_COLUMNS = ("GEOID20", "GEOID", "GEOCODE")
NAME_COLUMNS = ("NAME", "NAMELSAD20", "NAMELSAD", "NAME20")
# GEOID width per resolution; widths one short mean a numeric read dropped the
# leading zero of a state FIPS code below 10.
GEOID_WIDTHS = {"tract": 11, "block": 15}
GEOMETRY_FILE = "geometry.gpkg"


class RdhImportError(ValueError):
    """The given files can't be turned into a cache entry."""


@dataclass(frozen=True)
class RdhImportResult:
    state_fips: str
    resolution: str
    rows: int
    total_population: int
    census_csv: str
    geometry_path: str


@dataclass
class _Layer:
    source: Path
    frame: pd.DataFrame
    geoids: pd.Series
    state_fips: str
    resolution: str

    @property
    def has_counts(self) -> bool:
        columns = set(self.frame.columns)
        return "P0010001" in columns or "P1_001N" in columns

    @property
    def has_geometry(self) -> bool:
        return isinstance(self.frame, gpd.GeoDataFrame) and self.frame.geometry.notna().any()


def import_rdh_files(
    paths: Iterable[str | os.PathLike],
    state_fips: Optional[str] = None,
    resolution: Optional[str] = None,
    cache_dir: str = CACHE_DIR,
) -> List[RdhImportResult]:
    """Import RDH P.L. 94-171 files into the Census cache.

    State and resolution are read from the GEOIDs. When ``state_fips`` or
    ``resolution`` is given, every input must match it.
    """
    paths = list(paths)
    if not paths:
        raise RdhImportError("No files given to import.")
    with tempfile.TemporaryDirectory(prefix="rdh-import-") as workdir:
        layers = [layer for path in _expand_inputs(paths, Path(workdir))
                  if (layer := _read_layer(path)) is not None]
        if not layers:
            raise RdhImportError(
                "No tract or block layer with a GEOID20/GEOID column was found. "
                "Download the shapefile or CSV version of RDH's 2020 PL 94-171 "
                "tract (pl2020_t) or block (pl2020_b) file."
            )
        groups: Dict[Tuple[str, str], List[_Layer]] = {}
        for layer in layers:
            groups.setdefault((layer.state_fips, layer.resolution), []).append(layer)

        for found_state, found_resolution in groups:
            if state_fips and found_state != state_fips:
                raise RdhImportError(
                    f"Files are for state FIPS {found_state}, not {state_fips}."
                )
            if resolution and found_resolution != resolution:
                raise RdhImportError(
                    f"Files are at {found_resolution} level, not {resolution}."
                )

        return [
            _import_group(state, res, group, cache_dir)
            for (state, res), group in sorted(groups.items())
        ]


def _expand_inputs(paths: Iterable[str | os.PathLike], workdir: Path) -> List[Path]:
    """Return the .shp and .csv files inside the given files, zips and folders."""
    found: List[Path] = []
    for raw in paths:
        path = Path(raw).expanduser()
        if not path.exists():
            raise RdhImportError(f"File not found: {path}")
        if path.is_dir():
            found.extend(_expand_inputs(sorted(p for p in path.iterdir()), workdir))
        elif path.suffix.lower() == ".zip":
            target = Path(tempfile.mkdtemp(dir=workdir))
            try:
                with zipfile.ZipFile(path) as archive:
                    archive.extractall(target)
            except zipfile.BadZipFile as exc:
                raise RdhImportError(f"{path} is not a valid zip file.") from exc
            found.extend(_expand_inputs([target], workdir))
        elif path.suffix.lower() in {".shp", ".csv"}:
            found.append(path)
    return found


def _read_layer(path: Path) -> Optional[_Layer]:
    try:
        if path.suffix.lower() == ".csv":
            frame = pd.read_csv(path, dtype=str)
        else:
            frame = gpd.read_file(path)
    except Exception as exc:
        raise RdhImportError(f"Could not read {path.name}: {exc}") from exc

    geoid_column = next((c for c in GEOID_COLUMNS if c in frame.columns), None)
    if geoid_column is None:
        logger.info("Skipping %s: no GEOID column.", path.name)
        return None
    geoids = _normalize_geoids(frame[geoid_column])
    width = int(geoids.str.len().max()) if len(geoids) else 0
    resolution = next((res for res, w in GEOID_WIDTHS.items() if w == width), None)
    if resolution is None:
        logger.info(
            "Skipping %s: %d-digit GEOIDs are neither tracts (11) nor blocks (15).",
            path.name, width,
        )
        return None
    states = geoids.str[:2].unique()
    if len(states) != 1:
        raise RdhImportError(
            f"{path.name} covers several states ({', '.join(sorted(states))}); "
            "import one state at a time."
        )
    if geoids.duplicated().any():
        raise RdhImportError(f"{path.name} has duplicate GEOIDs.")
    return _Layer(path, frame, geoids, states[0], resolution)


def _normalize_geoids(series: pd.Series) -> pd.Series:
    """Return GEOIDs as zero-padded strings, undoing numeric reads."""
    if series.isna().any():
        raise RdhImportError("Some rows have no GEOID.")
    if pd.api.types.is_numeric_dtype(series):
        series = series.astype("int64")
    text = series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    longest = int(text.str.len().max()) if len(text) else 0
    for width in GEOID_WIDTHS.values():
        if longest in (width - 1, width):
            return text.str.zfill(width)
    return text


def _import_group(state_fips: str, resolution: str, layers: List[_Layer],
                  cache_dir: str) -> RdhImportResult:
    label = f"state {state_fips} at {resolution} level"
    counts_layer = next((layer for layer in layers if layer.has_counts), None)
    if counts_layer is None:
        raise RdhImportError(
            f"No population counts for {label}: expected a P0010001 column. "
            "Use RDH's PL 94-171 file, not a boundary-only file."
        )
    # Prefer geometry from the same file as the counts.
    geometry_layer = next(
        (layer for layer in [counts_layer, *layers] if layer.has_geometry), None
    )
    if geometry_layer is None:
        raise RdhImportError(
            f"No geometry for {label}: add the shapefile version of the RDH "
            "download, or a boundary shapefile for the same geography."
        )

    census_df = _census_frame(counts_layer)
    geometry = _geometry_frame(geometry_layer, set(census_df["GEOID"]), label)

    geometry_dir = shapefile_cache_dir(state_fips, resolution, cache_dir)
    if os.path.isdir(geometry_dir):
        shutil.rmtree(geometry_dir)
    os.makedirs(geometry_dir)
    geometry_path = os.path.join(geometry_dir, GEOMETRY_FILE)
    geometry.to_file(geometry_path, driver="GPKG")

    sources = sorted({counts_layer.source.name, geometry_layer.source.name})
    imported_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with open(os.path.join(geometry_dir, IMPORT_MANIFEST), "w", encoding="utf-8") as fp:
        json.dump(
            {
                "source": "Redistricting Data Hub",
                "geometry": GEOMETRY_FILE,
                "input_files": sources,
                "imported_at": imported_at,
            },
            fp,
            indent=2,
        )

    csv_path = _write_census_cache(census_df, state_fips, resolution, cache_dir)
    total_population = int(census_df["P1_001N"].sum())
    record_cache_index(
        {
            "state_fips": state_fips,
            "resolution": resolution,
            "rows": len(census_df),
            "total_population": total_population,
            "shapefile": geometry_path,
            "created_at": imported_at,
            "source": "Redistricting Data Hub",
            "input_files": sources,
        },
        cache_dir,
    )
    return RdhImportResult(
        state_fips=state_fips,
        resolution=resolution,
        rows=len(census_df),
        total_population=total_population,
        census_csv=csv_path,
        geometry_path=geometry_path,
    )


def _census_frame(layer: _Layer) -> pd.DataFrame:
    """Build the cached Census table, shaped like a Census API response."""
    frame = layer.frame.rename(
        columns={k: v for k, v in LEGACY_TO_API.items() if k in layer.frame.columns}
    )
    missing = [field for field in COUNT_FIELDS if field not in frame.columns]
    if missing:
        raise RdhImportError(
            f"{layer.source.name} is missing count columns: {', '.join(missing)}."
        )
    geoids = layer.geoids.reset_index(drop=True)
    census = pd.DataFrame({"GEOID": geoids})
    name_column = next((c for c in NAME_COLUMNS if c in frame.columns), None)
    census["NAME"] = (
        frame[name_column].astype(str).reset_index(drop=True) if name_column else ""
    )
    for field in COUNT_FIELDS:
        values = pd.to_numeric(frame[field].reset_index(drop=True), errors="coerce")
        if values.isna().any():
            raise RdhImportError(
                f"{layer.source.name} has blank or non-numeric values in {field}."
            )
        census[field] = values.astype("int64")
    census["state"] = geoids.str[:2]
    census["county"] = geoids.str[2:5]
    census["tract"] = geoids.str[5:11]
    if layer.resolution == "block":
        census["block"] = geoids.str[11:15]
    return census


def _geometry_frame(layer: _Layer, census_geoids: set, label: str) -> gpd.GeoDataFrame:
    frame = layer.frame
    if frame.crs is None:
        raise RdhImportError(
            f"{layer.source.name} has no coordinate system (missing .prj file)."
        )
    geometry = gpd.GeoDataFrame(
        {"GEOID": layer.geoids.to_numpy()},
        geometry=frame.geometry.to_numpy(),
        crs=frame.crs,
    )
    geometry = geometry[geometry.geometry.notna()]
    without_shapes = census_geoids - set(geometry["GEOID"])
    if without_shapes:
        example = sorted(without_shapes)[0]
        raise RdhImportError(
            f"{len(without_shapes)} units for {label} have counts but no geometry "
            f"(for example GEOID {example}). Use geometry from the same RDH download."
        )
    extra = ~geometry["GEOID"].isin(census_geoids)
    if extra.any():
        logger.warning("Dropping %d shapes with no population counts.", int(extra.sum()))
        geometry = geometry[~extra]
    return geometry.reset_index(drop=True)


def _write_census_cache(census_df: pd.DataFrame, state_fips: str, resolution: str,
                        cache_dir: str) -> str:
    os.makedirs(cache_dir, exist_ok=True)
    csv_path, parquet_path = census_cache_paths(state_fips, resolution, cache_dir)
    census_df.to_csv(csv_path, index=False)
    try:
        census_df.to_parquet(parquet_path, index=False)
    except Exception as exc:
        # Parquet needs pyarrow, which isn't a dependency. The fetcher reads
        # parquet first, so remove a stale one rather than hide the import.
        logger.info("Skipped %s (%s); the CSV cache is enough.", parquet_path, exc)
        if os.path.exists(parquet_path):
            os.remove(parquet_path)
    return csv_path
