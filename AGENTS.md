# Agent Guidance

## Project overview

Autoredistrict draws tract-level congressional maps that deliver a requested number of Democratic seats, for the 44
states with more than one seat (the six at-large states have no districts and are refused). It is a Java 28 application
(preview features on) with a JavaFX GUI and a picocli CLI. Both call the same service layer. The method is a Monte Carlo
simulated anneal after AlphaPhoenix's video "Algorithmic Redistricting: Elections made-to-order"; README.md describes
it.

## Modules

Each Gradle project is a named Java module `com.temotskipa.autoredistrict.<project>` (`engine` <- `core`; `cli` and
`gui` on top of `core`). `engine`, `cli` and `gui` each have one package of the same name; `core` has three.

- `engine`: pure Java, no I/O. `Graph` (CSR adjacency), `Districting` (chain state, incremental
  tallies, contiguity, scoring, the pruning move), `SingleFlip` behind `MoveKernel`, `SeatBands`, `Chain` (phases,
  temperatures, repair), `Annealer` (parallel seeds with `StructuredTaskScope` on platform threads).
- `core`, everything the two front ends share:
	- package `data`: `RdhTractImporter` (RDH zip, shapefile and CSV reading, GEOID padding, validation),
	  `Shapefile` (polygon shapefiles with dBASE tables, read and written), `AlbersProjection`, `AdjacencyBuilder`,
	  `Connectivity`, `BlockVotes`, `TractCache` (`.cache/rdh/<st>/`), `StateDataset`, `StateImporter`, `States`
	  (2020 apportionment).
	- package `service`: `PlanService` (run; refuses at-large states), `GraphVoronoi`, `Schedules`, `PlanMetrics` (PASS
	  checks), `SeatRange`, `BatchRunner`, `Artifacts`, `ImportService`. New features belong here, not in a front end.
	- package `render`: `MapPainter` over `Canvas2D` (Java2D here, JavaFX in the GUI), `MapRenderer`, `Exporters`.
- `cli`: `Main` with `import`, `run`, `all`, `seat-range`.
- `gui`: `Launcher` and `App`, including snapshot mode.

## Development guidelines

- Build with the Gradle wrapper: `.\gradlew.bat test` must pass before a change is done. Tests live in each module's
  `src/test/java`; fixtures are small synthetic files in `src/test/resources` or built by the tests. Tests check
  behaviour, not implementation details such as whether a type is a value class.
- The toolchain is JDK 28 EA in `~/.jdks`. `--enable-preview` and the launch flags live in the `build-logic` included
  build (`JvmFlags`, the `autoredistrict.java-conventions` and `autoredistrict.application-conventions` plugins); don't
  add them per module.
- JPMS: every module has a `module-info.java`; the apps run on the module path. A new dependency needs a `requires`
  (and `opens` if it reflects on our classes, like Jackson, picocli and JavaFX). Tests have no module-info and run on
  the class path. Every dependency must be an explicit module so jlink can link the apps; JTS gets its `module-info`
  from the `extraJavaModuleInfo` block in `autoredistrict.java-conventions`.
- Java conventions: standard Java naming, 4-space indents, `value record` for small immutable types, records for data
  carriers, primitive arrays in the engine's hot loop (no allocation per step).
- A result with alternatives is a sealed interface over records (`BatchRunner.Outcome`, `StateImporter.ImportResult`),
  read with exhaustive `switch`es and record patterns, not status strings or nullable error fields. Prefer pattern
  `switch` over `instanceof` chains, and text blocks for multi-line text.
- `var` for a local whose initializer names its type (`var sb = new StringBuilder()`, `var a = new int[n]`); keep the
  type written out in `List<String> l = new ArrayList<>()` style declarations (interface type or diamond).
- Imports follow IntelliJ's default settings, as its Optimize Imports leaves them: `*` from five classes of one package
  (always for `java.awt`) and from three static members, otherwise single-type imports. Import a class rather than
  writing its qualified name in code, unless the simple name is already taken (as `java.time.Duration` takes
  `Duration` in the GUI).
- Keep front ends thin. Logic shared by the GUI and the CLI goes in `core`'s `service` package.
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
