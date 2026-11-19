#!/usr/bin/env python3
"""
Headless CLI runner for the redistricting pipeline.

Outputs key plan metrics and saves the rendered map, report, and optional
shapefile so results can be inspected without the web UI.
"""
import argparse
import json
import logging
import os
import shutil
import sys
from datetime import datetime
from typing import List, Tuple

import geopandas as gpd
import pandas as pd
import us

from .core.apportionment import calculate_apportionment
from .data.census_fetcher import (
    DataFetcherWorker,
    has_cached_census_data,
    record_cache_index,
)
from .data.data_fetcher import DataFetcher
from .data.rdh_import import RdhImportError, import_rdh_files
from .rendering.map_generator import MapGenerator
from .services.redistricting_service import (
    approximate_district_count,
    build_demo_dataset,
    merge_census_geodata,
    normalize_state_fips,
    solve_geodata_plan,
)


DEMO_STATE_FIPS = "00"


def _state_fips(arg: str) -> str:
    """Return 2-digit state FIPS from name/abbr/fips input."""
    if arg.strip().lower() == "demo":
        return DEMO_STATE_FIPS
    try:
        return normalize_state_fips(arg)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Headless runner and smoke-test harness for the redistricting app."
    )
    parser.add_argument(
        "state",
        nargs="?",
        default="demo",
        type=_state_fips,
        help="State (name, abbr, or FIPS). Use 'demo' for synthetic smoke tests.",
    )
    parser.add_argument(
        "--mode",
        choices=["smoke", "demo", "live"],
        default="smoke",
        help="smoke=run demo and assert basics; demo=demo data only; live=real state.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("CENSUS_API_KEY"),
        help="Census API key (env CENSUS_API_KEY also honored).",
    )
    parser.add_argument(
        "--districts",
        type=int,
        default=None,
        help="Number of districts; defaults to apportioned value if available.",
    )
    parser.add_argument(
        "--algorithm",
        choices=["fair", "gerrymander", "gerrymander_dem", "gerrymander_rep"],
        default="fair",
        help="Redistricting algorithm to use.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Random seed for reproducible graph-solver runs.",
    )
    parser.add_argument(
        "--vra",
        action="store_true",
        help="Enable Voting Rights Act compliance heuristic.",
    )
    parser.add_argument(
        "--pop-weight",
        type=float,
        default=1.0,
        help="Population equality weight (0-1).",
    )
    parser.add_argument(
        "--compactness-weight",
        type=float,
        default=1.0,
        help="Compactness weight (0-1).",
    )
    parser.add_argument(
        "--election-year",
        type=int,
        default=None,
        help="Election year for partisan data (falls back to latest).",
    )
    parser.add_argument(
        "--provider",
        help="Manual partisan provider key (see provider_sources.yaml).",
    )
    parser.add_argument(
        "--resolution",
        choices=["block", "tract"],
        default=None,
        help="Spatial resolution for Census/shapefiles (default: block). "
             "'tract' is faster but lower fidelity.",
    )
    parser.add_argument(
        "--import-rdh",
        nargs="+",
        metavar="PATH",
        help="Import Redistricting Data Hub 2020 PL 94-171 downloads (zip, folder, "
             "shapefile or CSV) into the cache and exit. State and resolution are "
             "read from the files. Imported states run without a Census API key.",
    )
    parser.add_argument(
        "--cache-only",
        action="store_true",
        help="Fetch data/shapefiles, populate cache, and exit (no redistricting run).",
    )
    parser.add_argument(
        "--coi-csv",
        help="Optional COI CSV containing GEOID column; enforces preservation.",
    )
    parser.add_argument(
        "--map-out",
        default="output_map.png",
        help="Path to save rendered PNG (default: output_map.png).",
    )
    parser.add_argument(
        "--shp-out",
        default=None,
        help="Optional path to save districts as ESRI Shapefile.",
    )
    parser.add_argument(
        "--report-out",
        default=None,
        help="Optional path to save the full JSON plan report.",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run with synthetic demo data (no network/API key needed).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce output (useful for CI smoke runs).",
    )
    parser.add_argument(
        "--smoke-allow-demo",
        action="store_true",
        help="Allow smoke mode to fall back to demo data if live cache missing.",
    )
    return parser


def _print_metrics(metrics: List[Tuple]):
    if not metrics:
        print("No districts produced.")
        return
    header = f"{'ID':>3} | {'Pop':>12} | {'Dev %':>7} | {'Polsby':>7} | {'Partisan(D)':>11}"
    print(header)
    print("-" * len(header))
    for idx, pop, dev, compact, partisan in metrics:
        print(
            f"{idx:>3} | {int(pop):>12} | {dev:>7.2f} | {compact:>7.3f} | {partisan:>11.3f}"
        )


def _load_coi_geoids(path: str | None):
    if not path:
        return None
    frame = pd.read_csv(path, dtype=str)
    if "GEOID" not in frame.columns:
        raise RuntimeError("COI CSV must contain a GEOID column.")
    return frame["GEOID"].dropna().astype(str).tolist()


def main(argv=None):
    argv = argv or sys.argv[1:]
    parser = _build_parser()
    args = parser.parse_args(argv)

    # Load API key from config.json if not provided
    if not args.api_key:
        try:
            with open("config.json", "r") as fp:
                cfg = json.load(fp)
                args.api_key = cfg.get("api_key", args.api_key)
        except Exception:
            pass

    logging.basicConfig(level=logging.INFO if not args.quiet else logging.WARNING, format="%(levelname)s: %(message)s")

    requested_resolution = args.resolution
    args.resolution = args.resolution or "block"

    if args.import_rdh:
        try:
            results = import_rdh_files(
                args.import_rdh,
                state_fips=None if args.state == DEMO_STATE_FIPS else args.state,
                resolution=requested_resolution,
            )
        except RdhImportError as exc:
            parser.error(str(exc))
        for item in results:
            print(
                f"Imported state {item.state_fips} at {item.resolution} level: "
                f"{item.rows} units, population {item.total_population:,}."
            )
        return

    # Determine data mode
    demo_mode = args.demo or args.mode == "demo"
    live_mode = args.mode == "live"
    if live_mode and not args.api_key and not has_cached_census_data(args.state, args.resolution):
        parser.error(
            f"No cached Census data for state {args.state} at {args.resolution} level. "
            "Pass a Census API key (--api-key or CENSUS_API_KEY), or import "
            "Redistricting Data Hub files with --import-rdh."
        )

    # In smoke mode, prefer a real small state; require cache unless explicitly allowed to demo
    # argparse runs the default "demo" through _state_fips, so compare FIPS.
    if args.mode == "smoke" and args.state == DEMO_STATE_FIPS:
        args.state = us.states.ME.fips  # Maine (2 districts), small geography
        args.resolution = "tract"
        if has_cached_census_data(args.state, args.resolution):
            demo_mode = False
        else:
            if args.smoke_allow_demo:
                print("Smoke: cache missing for real state; falling back to demo dataset (--smoke-allow-demo used).")
                demo_mode = True
            else:
                print("Smoke: cache missing for real state; using demo dataset for now.")
                demo_mode = True

    if demo_mode:
        merged_gdf = build_demo_dataset(size=4, rich=True)
        shapefile_path = None
    else:
        provider_keys = [args.provider] if args.provider else None
        try:
            worker = DataFetcherWorker(
                args.state,
                args.api_key,
                election_year=args.election_year,
                provider_keys=provider_keys,
                resolution=args.resolution,
            )
            print("Fetching census data and shapefiles (may take a few minutes)...")
            census_df = worker._get_census_data(args.state)
            if census_df is None:
                raise RuntimeError("Failed to fetch census data.")
            shapefile_path = worker._get_shapefiles(args.state)
            if not shapefile_path:
                raise RuntimeError("Failed to fetch shapefiles.")

            merged_gdf = merge_census_geodata(shapefile_path, census_df)

            if args.cache_only:
                total_pop = census_df["P1_001N"].sum() if "P1_001N" in census_df else None
                meta = {
                    "state_fips": args.state,
                    "resolution": args.resolution,
                    "rows": len(census_df),
                    "total_population": int(total_pop) if total_pop is not None else None,
                    "shapefile": shapefile_path,
                    "created_at": datetime.utcnow().isoformat() + "Z",
                    "provider_keys": provider_keys,
                }
                record_cache_index(meta)
                print(f"Cache populated for state {args.state} at {args.resolution} resolution.")
                return
        except Exception as exc:
            if args.mode == "smoke":
                print(f"Smoke: live fetch failed ({exc}); falling back to demo dataset.")
                merged_gdf = build_demo_dataset(size=4, rich=True)
                demo_mode = True
            else:
                parser.error(str(exc))

    num_districts = args.districts
    if num_districts is None:
        if demo_mode:
            num_districts = 4
        else:
            try:
                pops = DataFetcher(args.api_key).get_all_states_population_data()
                if pops:
                    apportionment = calculate_apportionment(pops, 435)
                    num_districts = apportionment.get(args.state)
            except Exception:
                num_districts = None
    if num_districts is None:
        num_districts = approximate_district_count(merged_gdf, args.state)

    print(f"Running graph solver for {num_districts} districts...")
    result = solve_geodata_plan(
        state_data=merged_gdf,
        district_count=num_districts,
        algorithm=args.algorithm,
        population_equality_weight=args.pop_weight,
        compactness_weight=args.compactness_weight,
        vra_compliance=args.vra,
        communities_of_interest=_load_coi_geoids(args.coi_csv),
        random_seed=args.seed,
        plan_prefix="cli-demo" if demo_mode else f"cli-{args.state}",
        source_metadata={
            "mode": "demo" if demo_mode else "live",
            "state_fips": args.state,
            "resolution": args.resolution,
            "shapefile_path": shapefile_path,
            "vra_requested": bool(args.vra),
            "random_seed": args.seed,
        },
    )

    shutil.copyfile(result["artifacts"]["map_png"], args.map_out)
    print(f"Map saved to {args.map_out}")
    if args.shp_out:
        district_gdf = gpd.read_file(result["artifacts"]["districts_geojson"])
        mg = MapGenerator(district_gdf)
        mg.export_to_shapefile(args.shp_out)
        print(f"Shapefile saved to {args.shp_out}")
    if args.report_out:
        shutil.copyfile(result["artifacts"]["report_json"], args.report_out)
        print(f"Report saved to {args.report_out}")

    metrics = [
        (
            item["district_id"],
            item["population"],
            item["deviation_pct"],
            item["compactness_polsby_popper"],
            item["partisan_dem_share"],
        )
        for item in result["metrics"]
    ]
    if not args.quiet:
        print("\nDistrict metrics:")
        _print_metrics(metrics)
        print("\nTip: open the PNG or load the GeoJSON/shapefile in a GIS viewer to inspect the lines.")
        print(
            "\nValidation: "
            f"max deviation {result['summary']['max_abs_deviation_pct']:.2f}%, "
            f"avg compactness {result['summary']['average_compactness']:.3f}, "
            f"all contiguous {result['summary']['all_contiguous']}"
        )
        print(f"Artifact bundle: {os.path.dirname(result['artifacts']['report_json'])}")

    # Smoke-test assertions (lightweight)
    if args.mode == "smoke":
        assert result["summary"]["district_count"] == num_districts, "Incorrect number of districts"
        assert result["summary"]["max_abs_deviation_pct"] <= 2.0, "Population deviation too high"
        partisan_vals = [round(p, 2) for *_, p in metrics]
        assert len(set(partisan_vals)) >= 2, "Expected partisan variation across districts"
        assert result["summary"]["all_contiguous"], "At least one district is not contiguous"
        # COI check: force top-left 3 cells to stay together
        coi_list = [g for g in merged_gdf["GEOID"].head(3).tolist()]
        coi_result = solve_geodata_plan(
            state_data=merged_gdf,
            district_count=num_districts,
            algorithm="fair",
            population_equality_weight=args.pop_weight,
            compactness_weight=args.compactness_weight,
            communities_of_interest=coi_list,
            random_seed=0,
            plan_prefix="cli-smoke-coi",
            source_metadata={"mode": "smoke-coi"},
        )
        district_map = coi_result["assignment"]
        assert len({district_map[g] for g in coi_list}) == 1, "COI group not preserved"
        print("Smoke test passed (contiguity, COI, deviation checks).")


if __name__ == "__main__":
    main()
