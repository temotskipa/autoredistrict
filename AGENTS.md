# Agent Guidance

## Project overview

Autoredistrict draws tract-level congressional maps that deliver a requested number of Democratic seats, for all 50
states. It is a Java 28 application (preview features on) with a JavaFX GUI and a picocli CLI. Both call the same
service layer. The method is a Monte Carlo simulated anneal after AlphaPhoenix's video "Algorithmic Redistricting:
Elections made-to-order"; README.md describes it.

## Modules

Each Gradle project is a named Java module `com.temotskipa.autoredistrict.<project>` with one package of the same
name (`engine` <- `data` <- `service` -> `render`; `cli` and `gui` on top of `service`).

- `engine`: pure Java, no I/O and no GeoTools. `Graph` (CSR adjacency), `Districting` (chain state, incremental
  tallies, contiguity, scoring, the pruning move), `SingleFlip` behind `MoveKernel`, `SeatBands`, `Chain` (phases,
  temperatures, repair), `Annealer` (parallel seeds with `StructuredTaskScope` on platform threads).
- `data`: `RdhTractImporter` (GeoTools shapefile/CSV reading, GEOID padding, validation), `AlbersProjection`,
  `AdjacencyBuilder`, `Connectivity`, `BlockVotes`, `TractCache` (`.cache/rdh/<st>/`), `StateDataset`, `StateImporter`,
  `States` (2020 apportionment).
- `service`: `PlanService` (run, at-large short cut), `GraphVoronoi`, `Schedules`, `PlanMetrics` (PASS checks),
  `SeatRange`, `BatchRunner`, `Artifacts`, `ImportService`. New features belong here, not in a front end.
- `render`: `MapPainter` over `Canvas2D` (Java2D here, JavaFX in the GUI), `MapRenderer`, `Exporters`.
- `cli`: `Main` with `import`, `run`, `all`, `seat-range`.
- `gui`: `Launcher` and `App`, including snapshot mode.

## Development guidelines

- Build with the Gradle wrapper: `.\gradlew.bat test` must pass before a change is done. Tests live in each module's
  `src/test/java`; fixtures are small synthetic files in `src/test/resources` or built by the tests.
- The toolchain is JDK 28 EA in `~/.jdks`. `--enable-preview` and the launch flags live in the `build-logic` included
  build (`JvmFlags`, the `autoredistrict.java-conventions` and `autoredistrict.application-conventions` plugins); don't
  add them per module.
- JPMS: every module has a `module-info.java`; the apps run on the module path. A new dependency needs a `requires`
  (and `opens` if it reflects on our classes, like Jackson, picocli and JavaFX). Tests have no module-info and run on
  the class path. GeoTools and JTS are automatic modules; `data` and `render` list the JDK modules GeoTools needs.
- Java conventions: standard Java naming, 4-space indents, no wildcard imports in new code, `value record` for small
  immutable types, records for data carriers, primitive arrays in the engine's hot loop (no allocation per step).
- Keep front ends thin. Logic shared by the GUI and the CLI goes in `service`.
- Handle file and parsing errors explicitly with messages that say what to do (`DataException`).
- Don't commit generated data: `.cache/`, `out/`, `build/` and `*.aot` are ignored.
- Runs must stay deterministic for a given seed (no time-dependent behaviour except the time budget).

## Data

- All geography and votes come from Redistricting Data Hub downloads imported with `autoredistrict import`. Runs read
  only the cache. Population comes from the PL tract files only.
- census.gov (the API, `www2` TIGER downloads and the website) returns Cloudflare 403 pages from some regions,
  including the user's. Don't assume live fetches work; nothing in the app contacts census.gov.

## Known limitations

- Votes are one presidential race, split from precincts to blocks by RDH; seat results are estimates.
- Some seat requests are geographically impossible; they are reported as misses.
- California has no 2024 block file at RDH and falls back to 2020.
