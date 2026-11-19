# Autoredistrict

Autoredistrict is a local-first congressional redistricting workbench. It fetches Census and TIGER geography for a state, generates district plans under population, compactness, VRA, community-of-interest, and partisan criteria, scores them, and exports maps and data for inspection.

## Status

- **Works today:** synthetic demo grids run end to end through the web UI, the API, and the CLI, producing a full artifact bundle (PNG map, GeoJSON, assignment CSV, metrics CSV, JSON report).
- **Not yet practical:** real states. The current graph solver (`src/core/graph_solver.py`) recomputes polygon geometry for every candidate move, so a 100-unit grid already takes about a minute. A state at tract level has thousands of units. Replacing the solver core is the next milestone (see [Roadmap](#roadmap)).

## Setup

Requires [uv](https://docs.astral.sh/uv/) and Python 3.14.

```bash
uv sync
```

## Web interface

```bash
uv run python -m src.webapp
```

Then open <http://127.0.0.1:8765>. The UI runs the synthetic demo solver by default and can request a live state run with a Census API key.

API routes:

- `GET /api/health`
- `POST /api/plans/demo`
- `POST /api/plans/state`
- `GET /api/plans/{plan_id}`

Artifacts are written to `.cache/web/plans/<plan_id>/`.

## Command line

Demo data, no network or API key needed:

```bash
uv run python -m src.cli --mode demo --districts 4 --seed 11 --map-out demo.png --report-out plan.json
```

Smoke run with built-in assertions (contiguity, population deviation, COI preservation):

```bash
uv run python -m src.cli --mode smoke --map-out smoke.png --quiet
```

Live state data (tract resolution is faster; `block` is the default):

```bash
uv run python -m src.cli "North Carolina" --mode live --api-key $CENSUS_API_KEY --resolution tract
```

Prefetch and cache Census data and shapefiles without solving:

```bash
uv run python -m src.cli "North Carolina" --mode live --api-key $CENSUS_API_KEY --cache-only
```

### Without census.gov

census.gov blocks some regions, so neither the Census API nor the TIGER shapefile downloads work there. [Redistricting Data Hub](https://redistrictingdatahub.org/) republishes the same 2020 P.L. 94-171 counts joined to Census geometry, and a free account can download them. Download a state's PL 94-171 file in shapefile form, for example `me_pl2020_t.zip` for tracts or `me_pl2020_b.zip` for blocks, then import it:

```bash
uv run python -m src.cli --import-rdh me_pl2020_t.zip
```

The importer reads the state and resolution from the GEOIDs and writes the same cache files a live fetch would. After that, live runs for that state need no API key and make no request to census.gov, in both the CLI and the web UI:

```bash
uv run python -m src.cli ME --mode live --resolution tract
```

If you download RDH's CSV version instead, pass a boundary shapefile for the same geography along with it.

Run `uv run python -m src.cli --help` for every option, including `--vra`, `--coi-csv`, `--algorithm`, and `--shp-out`.

## Tests

```bash
uv run pytest
```

The suite takes a few minutes, almost all of it in solver runs on small grids.

## Project layout

| Path | Purpose |
|---|---|
| `src/api.py`, `src/webapp.py`, `src/web/` | FastAPI app, server entry point, browser UI |
| `src/cli.py` | Headless CLI and smoke harness |
| `src/services/redistricting_service.py` | Shared pipeline: data prep, solve, metrics, artifacts. Both front ends go through it. |
| `src/core/graph_solver.py` | Current district solver (multi-start growth, boundary annealing, pair recombination) |
| `src/core/apportionment.py` | Huntington-Hill apportionment |
| `src/core/utils.py` | Adjacency, contiguity, compactness, partisan share helpers |
| `src/data/census_fetcher.py` | Census API and TIGER shapefile download with caching |
| `src/data/data_fetcher.py` | Statewide populations for apportionment |
| `src/data/partisan_*.py`, `data/provider_sources.yaml` | Election-return providers for partisan scores |
| `src/rendering/map_generator.py` | Map PNG and shapefile export |
| `tools/generate_providers.py` | Regenerates provider metadata from OpenElections |

## Roadmap

1. Replace the solver core with a ReCom-based engine that keeps district totals in arrays and scores moves incrementally, without polygon unions inside the loop.
2. Run a real state end to end at tract level, then at block level.
3. Improve partisan data resolution. Scores are currently county-level, applied uniformly to every tract or block in a county.
