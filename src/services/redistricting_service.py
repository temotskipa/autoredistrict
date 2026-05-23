"""Reusable redistricting pipeline services.

This module keeps the web/API layer thin and gives tests a stable way to run
the same solve path that the UI uses.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import geopandas as gpd
import numpy as np
import pandas as pd
import us
from shapely.geometry import box

from ..core.graph_solver import GraphRedistrictingSolver
from ..core.redistricting_algorithms import _polsby_popper_static, _weighted_partisan_share
from ..core.utils import is_contiguous
from ..rendering.map_generator import MapGenerator
from ..workers.data_worker import DataFetcherWorker

ARTIFACT_ROOT = Path(".cache") / "web" / "plans"


@dataclass(frozen=True)
class DemoPlanRequest:
    districts: int = 4
    algorithm: str = "fair"
    population_equality_weight: float = 1.0
    compactness_weight: float = 1.0
    vra: bool = False
    preserve_demo_coi: bool = False
    grid_size: int = 4
    random_seed: int = 0


@dataclass(frozen=True)
class StatePlanRequest:
    state: str
    api_key: str
    districts: Optional[int] = None
    algorithm: str = "fair"
    population_equality_weight: float = 1.0
    compactness_weight: float = 1.0
    vra: bool = False
    resolution: str = "tract"
    election_year: Optional[int] = None
    provider_keys: Optional[Sequence[str]] = None
    random_seed: int = 0


def build_demo_dataset(size: int = 4, rich: bool = True) -> gpd.GeoDataFrame:
    """Return a deterministic synthetic grid for smoke tests and UI demos."""
    records: List[Dict[str, Any]] = []
    base_pop = 1000

    for i in range(size):
        for j in range(size):
            geoid = f"000{i:02d}{j:02d}"
            geom = box(i, j, i + 1, j + 1)
            pop = base_pop + (i * size + j) * 25
            if rich:
                minority = pop * (0.55 if (i + j) % 3 == 0 else 0.35)
            else:
                minority = pop * (0.40 if (i + j) % 2 == 0 else 0.20)
            partisan = 0.3 if i < (size / 2) else 0.7
            records.append(
                {
                    "GEOID": geoid,
                    "state": "00",
                    "county": f"{i:03d}",
                    "tract": f"{j:06d}",
                    "P1_001N": pop,
                    "P1_003N": pop - minority,
                    "partisan_score": partisan,
                    "geometry": geom,
                }
            )

    return gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")


def run_demo_plan(
    request: DemoPlanRequest,
    artifact_root: Path | str = ARTIFACT_ROOT,
) -> Dict[str, Any]:
    """Generate a demo plan and persist map/report artifacts."""
    grid_size = max(2, min(12, int(request.grid_size)))
    district_count = max(1, min(grid_size * grid_size, int(request.districts)))
    state_data = build_demo_dataset(size=grid_size, rich=True)
    communities = _demo_coi(state_data) if request.preserve_demo_coi else None
    return solve_geodata_plan(
        state_data=state_data,
        district_count=district_count,
        algorithm=request.algorithm,
        population_equality_weight=request.population_equality_weight,
        compactness_weight=request.compactness_weight,
        vra_compliance=request.vra,
        communities_of_interest=communities,
        random_seed=request.random_seed,
        artifact_root=artifact_root,
        plan_prefix="demo",
        source_metadata={
            "mode": "demo",
            "grid_size": grid_size,
            "vra_requested": bool(request.vra),
        },
    )


def run_state_plan(
    request: StatePlanRequest,
    artifact_root: Path | str = ARTIFACT_ROOT,
) -> Dict[str, Any]:
    """Fetch state data and run the graph solver through the shared pipeline."""
    state_fips = normalize_state_fips(request.state)
    if not request.api_key:
        raise ValueError("A Census API key is required for live state plans.")
    resolution = request.resolution if request.resolution in {"tract", "block"} else "tract"

    worker = DataFetcherWorker(
        state_fips,
        request.api_key,
        election_year=request.election_year,
        provider_keys=list(request.provider_keys) if request.provider_keys else None,
        resolution=resolution,
    )
    census_df = worker._get_census_data(state_fips)
    if census_df is None or census_df.empty:
        raise RuntimeError("Failed to fetch Census data for the requested state.")
    shapefile_path = worker._get_shapefiles(state_fips)
    if not shapefile_path:
        raise RuntimeError("Failed to fetch TIGER shapefiles for the requested state.")

    state_data = merge_census_geodata(shapefile_path, census_df)
    if state_data.empty:
        raise RuntimeError("No Census rows matched the shapefile GEOIDs.")
    district_count = request.districts or approximate_district_count(state_data)

    return solve_geodata_plan(
        state_data=state_data,
        district_count=district_count,
        algorithm=request.algorithm,
        population_equality_weight=request.population_equality_weight,
        compactness_weight=request.compactness_weight,
        vra_compliance=request.vra,
        communities_of_interest=None,
        random_seed=request.random_seed,
        artifact_root=artifact_root,
        plan_prefix=f"state-{state_fips}",
        source_metadata={
            "mode": "state",
            "state_fips": state_fips,
            "resolution": resolution,
            "vra_requested": bool(request.vra),
            "election_year": request.election_year,
            "provider_keys": list(request.provider_keys) if request.provider_keys else None,
            "shapefile_path": shapefile_path,
        },
    )


def solve_geodata_plan(
    state_data: gpd.GeoDataFrame,
    district_count: int,
    algorithm: str = "fair",
    population_equality_weight: float = 1.0,
    compactness_weight: float = 1.0,
    vra_compliance: bool = False,
    communities_of_interest: Optional[Sequence[str]] = None,
    random_seed: int = 0,
    artifact_root: Path | str = ARTIFACT_ROOT,
    plan_prefix: str = "plan",
    source_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Solve any prepared GeoDataFrame and persist a complete artifact bundle."""
    artifact_root = Path(artifact_root)
    artifact_root.mkdir(parents=True, exist_ok=True)

    algorithm_name = algorithm.lower()
    target_party: Optional[int] = None
    partisan_weight = 0.0
    if algorithm_name == "gerrymander_dem":
        target_party = 1
        partisan_weight = 1.0
    elif algorithm_name == "gerrymander_rep":
        target_party = 0
        partisan_weight = 1.0
    elif algorithm_name == "gerrymander":
        partisan_weight = 1.0

    solver = GraphRedistrictingSolver(
        state_data,
        district_count,
        population_equality_weight=_clamp_weight(population_equality_weight),
        compactness_weight=_clamp_weight(compactness_weight),
        partisan_weight=partisan_weight,
        vra_compliance=vra_compliance,
        communities_of_interest=communities_of_interest,
        target_party=target_party,
        random_seed=random_seed,
    )
    solved = solver.solve()
    districts = solved.districts

    all_gdf = _collect_districts(districts, state_data.crs)
    metrics = compute_district_metrics(districts)
    summary = summarize_metrics(metrics, requested_districts=district_count)
    warnings = _build_warnings(summary, district_count)

    plan_id = f"{plan_prefix}-{uuid.uuid4().hex[:12]}"
    plan_dir = artifact_root / plan_id
    plan_dir.mkdir(parents=True, exist_ok=True)

    map_path = plan_dir / "map.png"
    geojson_path = plan_dir / "districts.geojson"
    assignment_path = plan_dir / "assignment.csv"
    metrics_path = plan_dir / "metrics.csv"
    report_path = plan_dir / "plan.json"

    map_generator = MapGenerator(all_gdf)
    map_generator.generate_map_image(str(map_path))
    _write_geojson(map_generator, geojson_path)
    _write_assignment_csv(solved.assignment, assignment_path)
    _write_metrics_csv(metrics, metrics_path)

    result: Dict[str, Any] = {
        "plan_id": plan_id,
        "status": "best_found_graph",
        "algorithm": algorithm,
        "assignment": solved.assignment,
        "solver": solved.metadata,
        "source": source_metadata or {},
        "summary": summary,
        "metrics": metrics,
        "warnings": warnings,
        "artifacts": {
            "map_png": str(map_path),
            "districts_geojson": str(geojson_path),
            "assignment_csv": str(assignment_path),
            "metrics_csv": str(metrics_path),
            "report_json": str(report_path),
        },
    }
    report_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def merge_census_geodata(shapefile_path: str, census_df: pd.DataFrame) -> gpd.GeoDataFrame:
    state_gdf = gpd.read_file(shapefile_path)
    if "GEOID" not in state_gdf.columns:
        if "GEOID20" in state_gdf.columns:
            state_gdf["GEOID"] = state_gdf["GEOID20"]
        else:
            raise RuntimeError("Shapefile missing GEOID/GEOID20 field.")

    merged = state_gdf.merge(census_df, on="GEOID")
    if "partisan_score" not in merged.columns:
        merged["partisan_score"] = 0.5
    merged["partisan_score"] = pd.to_numeric(
        merged["partisan_score"], errors="coerce"
    )
    fallback = merged["partisan_score"].mean()
    if pd.isna(fallback):
        fallback = 0.5
    merged["partisan_score"] = merged["partisan_score"].fillna(fallback)
    return merged


def approximate_district_count(state_data: gpd.GeoDataFrame) -> int:
    total_population = pd.to_numeric(
        state_data["P1_001N"], errors="coerce"
    ).fillna(0).sum()
    return max(1, round(float(total_population) / 760_000))


def normalize_state_fips(state: str) -> str:
    state = str(state).strip()
    if state.isdigit():
        return state.zfill(2)
    looked_up = us.states.lookup(state)
    if not looked_up:
        raise ValueError(f"Unrecognized state: {state}")
    return looked_up.fips


def compute_district_metrics(districts: Iterable[gpd.GeoDataFrame]) -> List[Dict[str, Any]]:
    districts = list(districts)
    total_pop = sum(float(district["P1_001N"].sum()) for district in districts)
    ideal = total_pop / len(districts) if districts else 0.0

    metrics: List[Dict[str, Any]] = []
    for idx, district in enumerate(districts):
        pop = float(district["P1_001N"].sum())
        deviation_pct = 0.0 if ideal == 0 else ((pop - ideal) / ideal) * 100
        metrics.append(
            {
                "district_id": idx + 1,
                "population": int(round(pop)),
                "deviation_pct": round(deviation_pct, 3),
                "compactness_polsby_popper": round(float(_polsby_popper_static(district)), 4),
                "partisan_dem_share": round(float(_weighted_partisan_share(district)), 4),
                "contiguous": bool(is_contiguous(district)),
                "unit_count": int(len(district)),
            }
        )
    return metrics


def summarize_metrics(
    metrics: List[Dict[str, Any]],
    requested_districts: Optional[int] = None,
) -> Dict[str, Any]:
    if not metrics:
        return {
            "requested_districts": requested_districts,
            "district_count": 0,
            "total_population": 0,
            "ideal_population": 0,
            "max_abs_deviation_pct": None,
            "average_compactness": None,
            "all_contiguous": False,
        }

    total_pop = sum(item["population"] for item in metrics)
    district_count = len(metrics)
    return {
        "requested_districts": requested_districts or district_count,
        "district_count": district_count,
        "total_population": total_pop,
        "ideal_population": round(total_pop / district_count, 2),
        "max_abs_deviation_pct": round(
            max(abs(float(item["deviation_pct"])) for item in metrics),
            3,
        ),
        "average_compactness": round(
            float(np.mean([item["compactness_polsby_popper"] for item in metrics])),
            4,
        ),
        "all_contiguous": all(bool(item["contiguous"]) for item in metrics),
    }


def load_plan(plan_id: str, artifact_root: Path | str = ARTIFACT_ROOT) -> Optional[Dict[str, Any]]:
    report_path = Path(artifact_root) / plan_id / "plan.json"
    if not report_path.exists():
        return None
    return json.loads(report_path.read_text(encoding="utf-8"))


def _collect_districts(districts: Iterable[gpd.GeoDataFrame], crs: Any) -> gpd.GeoDataFrame:
    frames = []
    for idx, district in enumerate(districts, start=1):
        district = district.copy()
        district["district_id"] = idx
        frames.append(district)
    if not frames:
        return gpd.GeoDataFrame(columns=["district_id", "geometry"], geometry="geometry", crs=crs)
    return gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), geometry="geometry", crs=crs)


def _write_geojson(map_generator: MapGenerator, output_path: Path) -> None:
    dissolved = map_generator._dissolved_districts().copy()
    if dissolved.crs and dissolved.crs.is_geographic is False:
        dissolved = dissolved.to_crs(epsg=4326)
    output_path.write_text(dissolved.to_json(), encoding="utf-8")


def _write_assignment_csv(assignment: Dict[str, int], output_path: Path) -> None:
    rows = [
        {"GEOID": geoid, "district_id": district_id}
        for geoid, district_id in sorted(assignment.items())
    ]
    pd.DataFrame(rows).to_csv(output_path, index=False)


def _write_metrics_csv(metrics: List[Dict[str, Any]], output_path: Path) -> None:
    pd.DataFrame(metrics).to_csv(output_path, index=False)


def _demo_coi(state_data: gpd.GeoDataFrame) -> List[str]:
    return state_data["GEOID"].head(3).astype(str).tolist()


def _clamp_weight(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _build_warnings(summary: Dict[str, Any], requested_districts: int) -> List[str]:
    warnings: List[str] = []
    if summary["district_count"] != requested_districts:
        warnings.append(
            f"Solver returned {summary['district_count']} districts for a request of {requested_districts}."
        )
    if not summary["all_contiguous"]:
        warnings.append("At least one district is not contiguous.")
    max_dev = summary.get("max_abs_deviation_pct")
    if max_dev is not None and max_dev > 1.0:
        warnings.append(
            "Population deviation exceeds 1% with the current atomic units; "
            "use finer source geography for congressional-grade equality."
        )
    return warnings
