# Agent Guidance

## Project overview

Autoredistrict is a local-first congressional redistricting workbench written in Python. It has two front ends, a
FastAPI web app with a browser UI and a headless CLI. Both run the same pipeline in
`src/services/redistricting_service.py`. Geographic data is handled with GeoPandas and Shapely.

## Key modules

- `src/api.py`: FastAPI routes; serves the UI from `src/web/` and plan artifacts from `.cache/web/plans/`.
- `src/webapp.py`: uvicorn entry point (`python -m src.webapp`).
- `src/cli.py`: CLI entry point (`python -m src.cli`), including `demo`, `smoke`, and `live` modes.
- `src/services/redistricting_service.py`: demo dataset, Census/TIGER merge, solve, metrics, warnings, artifact writing.
  New features belong here, not in a front end.
- `src/core/graph_solver.py`: `GraphRedistrictingSolver`, the current solver.
- `src/core/utils.py`: adjacency, contiguity, Polsby-Popper, and population-weighted partisan share.
- `src/core/apportionment.py`: Huntington-Hill apportionment.
- `src/data/census_fetcher.py`: `DataFetcherWorker`, which downloads Census P1 data and TIGER shapefiles with retries
  and caching under `.cache/`. It creates the Census client only when a download needs it, so cached states run without
  an API key.
- `src/data/rdh_import.py`: imports Redistricting Data Hub PL 94-171 downloads into that same cache, for places where
  census.gov is blocked (`python -m src.cli --import-rdh <zip>`).
- `src/data/partisan_providers.py`, `src/data/partisan_data.py`: election-return providers, registered from
  `data/provider_sources.yaml`.
- `src/rendering/map_generator.py`: map PNG and shapefile export.

## Development guidelines

- Use uv: `uv sync`, then `uv run pytest`. Tests live in `tests/` and must pass before a change is done.
- Follow PEP 8.
- Keep front ends thin. Logic shared by the web app and the CLI goes in the service layer.
- Handle network and file errors explicitly. Census and TIGER downloads fail intermittently.

## Known limitations

- The graph solver does not scale. It rebuilds GeoDataFrames and runs polygon unions for every scored move, so runtime
  grows quickly with unit count. Replacing it is the top priority.
- Partisan scores are county-level and applied uniformly to every tract or block in a county.

## API usage notes

- Census API block-level requests must specify both state and county FIPS codes. A request for all blocks in a state
  without a county fails.
- census.gov (the API, `www2` TIGER downloads and the website) returns Cloudflare 403 pages from some regions, including
  the user's. Don't assume live fetches work; prefer cached or imported data in tests and checks.
