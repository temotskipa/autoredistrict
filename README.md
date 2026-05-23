# Congressional Redistricting Application

Autoredistrict is a local-first congressional redistricting workbench. It can generate reproducible district plans from synthetic demo geography or live Census/TIGER state data, score the plans with district metrics, and export map and data artifacts for inspection.

## Features

- **Automatic Redistricting:** Generate congressional district maps from demo or live state geography.
- **Graph Solver:** Build contiguous districts over an adjacency graph with deterministic multi-start growth, local boundary moves, annealing, and adjacent-pair recombination.
- **Multiple Algorithms:** Choose a fair baseline or Democratic/Republican advantage modes.
- **Customizable Criteria:** Adjust parameters like population equality and compactness.
- **VRA and COI Heuristics:** Enable Voting Rights Act opportunity-district scoring and preserve communities of interest by GEOID.
- **Data Fetching:** Automatically fetches the latest census data and shapefiles.
- **Local Web/API Mode:** Run a FastAPI server with a browser UI for generating and reviewing plans.
- **Export Options:** Export PNG maps, dissolved GeoJSON, unit assignment CSV, metrics CSV, JSON reports, and optional shapefiles.

## Setup and Installation

1. **Clone the repository:**

    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2. **Install the dependencies:**
    Make sure you have Python 3 installed. Then, run the following command to install the necessary libraries:

    ```bash
    pip install -r requirements.txt
    ```

## How to Run

To run the legacy desktop application, execute the `src.app` module:

```bash
python -m src.app
```

This will open the main application window, where you can select a state, choose an algorithm, and generate a district map.

### Local Web Interface

The browser-based local interface is the preferred path for ongoing development and automated testing:

```bash
uv run python -m src.webapp
```

Then open <http://127.0.0.1:8765>. The current web UI runs the graph-based synthetic demo solver by default, can request a live state run with a Census API key, displays the generated map and district metrics, and exposes PNG, GeoJSON, assignment CSV, metrics CSV, and JSON report artifacts.

Useful API routes:

- `GET /api/health`
- `POST /api/plans/demo`
- `POST /api/plans/state`
- `GET /api/plans/{plan_id}`

### Headless CLI (no GUI required)

For quick smoke tests or server use, run the CLI:

```bash
python -m src.cli <state> --api-key $CENSUS_API_KEY --map-out map.png --shp-out districts.shp
```

To exercise the pipeline without network/API keys, use the synthetic demo dataset:

```bash
python -m src.cli demo --demo --districts 4 --seed 11 --map-out demo.png --report-out plan.json
```

For faster but lower-fidelity runs, use tract resolution:

```bash
python -m src.cli "North Carolina" --api-key $CENSUS_API_KEY --resolution tract
```

To prefetch and cache data/shapefiles only (no map generation):

```bash
python -m src.cli "North Carolina" --api-key $CENSUS_API_KEY --cache-only
```

For a no-network smoke run with assertions:

```bash
uv run python -m src.cli --demo --mode smoke --map-out smoke.png --quiet
```

## Verification

Run the automated test suite:

```bash
uv run --frozen pytest
```

The current tests cover API health and artifact serving, demo plan generation, mocked live-state solving, deterministic graph-solver behavior, contiguity, VRA/COI heuristics, recombination population-balance improvement, and CLI report/map output.
