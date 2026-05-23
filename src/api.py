"""Local web API for the autoredistrict browser interface."""

from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .services.redistricting_service import (
    ARTIFACT_ROOT,
    DemoPlanRequest,
    StatePlanRequest,
    load_plan,
    run_demo_plan,
    run_state_plan,
)

WEB_DIR = Path(__file__).resolve().parent / "web"
ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
mimetypes.add_type("application/geo+json", ".geojson")
mimetypes.add_type("text/csv", ".csv")

app = FastAPI(
    title="Autoredistrict Local API",
    description="Local-first API for automated congressional redistricting workflows.",
    version="0.1.0",
)

app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")
app.mount("/artifacts", StaticFiles(directory=str(ARTIFACT_ROOT)), name="artifacts")


class DemoPlanPayload(BaseModel):
    districts: int = Field(default=4, ge=1, le=144)
    algorithm: Literal["fair", "gerrymander", "gerrymander_dem", "gerrymander_rep"] = "fair"
    population_equality_weight: float = Field(default=1.0, ge=0.0, le=1.0)
    compactness_weight: float = Field(default=1.0, ge=0.0, le=1.0)
    vra: bool = False
    preserve_demo_coi: bool = False
    grid_size: int = Field(default=4, ge=2, le=12)
    random_seed: int = Field(default=0, ge=0, le=2_147_483_647)


class StatePlanPayload(BaseModel):
    state: str = Field(min_length=2, max_length=32)
    api_key: str = Field(min_length=1)
    districts: int | None = Field(default=None, ge=1, le=100)
    algorithm: Literal["fair", "gerrymander", "gerrymander_dem", "gerrymander_rep"] = "fair"
    population_equality_weight: float = Field(default=1.0, ge=0.0, le=1.0)
    compactness_weight: float = Field(default=1.0, ge=0.0, le=1.0)
    vra: bool = False
    resolution: Literal["tract", "block"] = "tract"
    election_year: int | None = None
    provider_keys: list[str] | None = None
    random_seed: int = Field(default=0, ge=0, le=2_147_483_647)


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(WEB_DIR / "index.html")


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return Response(status_code=204)


@app.get("/api/health")
def health():
    return {"status": "ok", "mode": "local-web"}


@app.post("/api/plans/demo")
def create_demo_plan(payload: DemoPlanPayload):
    result = run_demo_plan(
        DemoPlanRequest(
            districts=payload.districts,
            algorithm=payload.algorithm,
            population_equality_weight=payload.population_equality_weight,
            compactness_weight=payload.compactness_weight,
            vra=payload.vra,
            preserve_demo_coi=payload.preserve_demo_coi,
            grid_size=payload.grid_size,
            random_seed=payload.random_seed,
        )
    )
    return _with_artifact_urls(result)


@app.post("/api/plans/state")
def create_state_plan(payload: StatePlanPayload):
    try:
        result = run_state_plan(
            StatePlanRequest(
                state=payload.state,
                api_key=payload.api_key,
                districts=payload.districts,
                algorithm=payload.algorithm,
                population_equality_weight=payload.population_equality_weight,
                compactness_weight=payload.compactness_weight,
                vra=payload.vra,
                resolution=payload.resolution,
                election_year=payload.election_year,
                provider_keys=payload.provider_keys,
                random_seed=payload.random_seed,
            )
        )
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _with_artifact_urls(result)


@app.get("/api/plans/{plan_id}")
def get_plan(plan_id: str):
    result = load_plan(plan_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Plan not found.")
    return _with_artifact_urls(result)


def _with_artifact_urls(result):
    plan_id = result["plan_id"]
    result = dict(result)
    result["urls"] = {
        "map_png": f"/artifacts/{plan_id}/map.png",
        "districts_geojson": f"/artifacts/{plan_id}/districts.geojson",
        "assignment_csv": f"/artifacts/{plan_id}/assignment.csv",
        "metrics_csv": f"/artifacts/{plan_id}/metrics.csv",
        "report_json": f"/artifacts/{plan_id}/plan.json",
    }
    return result
