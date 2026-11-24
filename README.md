# Autoredistrict

Autoredistrict draws congressional maps that deliver a requested number of Democratic seats. You give it a state and
a seat count; it draws a valid tract-level map (equal population within 1%, every district contiguous) that produces
that result under one election's vote data. It works for all 50 states, runs locally on a JDK 28 desktop, and needs
no Census API key and no access to census.gov.

There is a JavaFX desktop GUI and a command line. Both call the same service layer.

"Fair" is not a goal of this tool. When no seat count is given, the default is the proportional count,
`round(D two-party share x seats)`, which is plain arithmetic, not a fairness claim. A seat result is an estimate:
who would win each district under one election's data, not a forecast.

## The method

The engine follows AlphaPhoenix's video
[Algorithmic Redistricting: Elections made-to-order](https://www.youtube.com/watch?v=Lq-Y7crQo44): a Markov chain
Monte Carlo simulated anneal that flips one unit at a time. Census tracts take the place of the video's pixels.

1. **Start** from a graph Voronoi map: k random seed tracts, and every tract joins the nearest seed by shortest path
   over centroid distances. Every district is contiguous from the start, and deliberately unbalanced.
2. **Move**: pick a boundary tract (tracts with more neighbours in other districts are picked more often, the video's
   "statistical surface tension") and flip it to a neighbouring district. Keep it if the score improves, or with
   probability exp(-delta/T) if it gets worse.
3. **Score** (lower is better), each term averaged over the districts:
   - population: the video's fourth-power spread of district populations;
   - compactness: each district's moment of inertia about its weighted centre, normalised by weight times area;
   - surface tension: boundary length shared with other districts;
   - election fit: districts sorted by D share; the top N must reach 50% + margin/2 and the rest stay below
     50% - margin/2. The penalty is the squared distance outside those bands, so districts near 50% carry the weight.
4. **Contiguity**: a flip that would split the district it leaves is rejected, checked with a fast neighbour test and
   otherwise a bounded simultaneous BFS. If the flip would only cut off a small piece (up to 40 tracts), the tract and
   that piece can move together, so a district with a one-tract-wide tail can still shrink.
5. **Tallies**: per-district population, votes, moment sums, area and boundary length are updated in O(degree + k)
   per flip and checked against a full recomputation at the end of every chain.
6. **Three phases**, as in the video, each with exponential cooling: *shape* (population and centring), *explore*
   (hotter, less surface tension, some election pressure) and *elect* (full election fit, surface tension back on).
   Population is also a hard rule in the last phase: a district inside the 1% tolerance may not leave it. If any
   district is still outside after the last phase, a repair run continues until all are inside.
7. **Seeds**: `--seeds S` runs S chains in parallel (structured concurrency) and keeps the best: population valid
   first, then hitting the target, then the lowest score.

Step counts scale with the number of tracts (6,000 + 2,400 extra proposals per tract at the default scale), so every
state gets the same effort per tract. Texas (6,896 tracts, 38 seats) takes about 20 s on 16 cores.

## Setup

- **JDK 28 early access** (`openjdk-ea-28-ea+25-2095`) in `~/.jdks`. Preview features are on everywhere. Gradle runs on
  any JDK 17+ and finds the toolchain in `~/.jdks` on its own; it never downloads one.
- The Gradle wrapper is committed: `.\gradlew.bat` downloads Gradle 9.9.2 and checks its SHA-256.

```powershell
.\gradlew.bat test
.\gradlew.bat :cli:installDist :gui:installDist
```

The start scripts in `cli\build\install\autoredistrict\bin` and `gui\build\install\autoredistrict-gui\bin` run on the
JDK 28 toolchain path recorded at build time; set `AUTOREDISTRICT_JAVA_HOME` to use another JDK 28.

## Data: two manual downloads

census.gov blocks some regions (Cloudflare 403), so all geography and votes come from the
[Redistricting Data Hub](https://redistrictingdatahub.org/). Put these files in one folder, for example
`C:\Users\you\Downloads\rdh`:

1. **Tracts with population**: RDH's 2020 PL 94-171 tract shapefile for each state, `xx_pl2020_t.zip` (50 files).
2. **Votes**:
   - `xx_2024_gen_2020_blocks_csv.zip`, "2024 General Election Results Disaggregated to the 2020 Block", for every
     state except California (RDH has only precinct results for CA in 2024);
   - `national_block_2020_pres_results.zip`, "2020 Presidential Results on Nationwide 2020 Census Blocks".

The vote data is precinct-level: RDH splits each precinct's votes among its blocks by voting-age population, and
autoredistrict sums the blocks to tracts. Each tract therefore has its own D and R totals, so the engine can separate
a county's Democratic neighbourhoods from its Republican ones. The limit is precinct size: blocks inside one precinct
share that precinct's split. Only the two major-party presidential columns are read (`G24PREDHAR`/`G24PRERTRU`,
Wisconsin's `PREDEM24`/`PREREP24`, and `G20PREDBID`/`G20PRERTRU`). Population always comes from the PL files.

## Workflow: import, then run

```powershell
$ar = "cli\build\install\autoredistrict\bin\autoredistrict.bat"
& $ar import --rdh-dir C:\Users\you\Downloads\rdh
```

`import` reads the zips once into `.cache\rdh\<st>\`: tracts, rook adjacency (shared boundary segments), bridges that
link islands and water gaps to their nearest neighbour, simplified map geometry, votes for 2024 and 2020, and a
manifest with the source files' SHA-256s. All 50 states take about 20 s. Missing files are listed by name. Runs read
only the cache.

```powershell
& $ar run NC --dem-seats 9 --out-dir out\nc
& $ar run TX                                   # proportional default
& $ar all --resolution tract --seed 1 --out-dir out\all
& $ar seat-range NC --out-dir out\seat-range
```

| Command | What it does |
|---|---|
| `import --rdh-dir DIR [--states ME,NC]` | Import RDH downloads into the cache. |
| `run ST` | Draw one map; prints the result and the district table, writes PNG, JSON report, assignment CSV, GeoJSON and a shapefile. |
| `all` | Every imported state at its proportional default: per-state maps and reports, `summary.csv`, `summary.md` and `contact-sheet.png`. A state that fails is recorded and the batch goes on. |
| `seat-range ST` | Request the proportional count, then step one seat at a time in each direction until a request misses. A hit also has to meet the PASS rules. |

Options for `run`, `all` and `seat-range`:

| Option | Default | Meaning |
|---|---|---|
| `--dem-seats N` | proportional | Democratic seats to deliver, 0..k (`run` only). |
| `--min-margin P` | 2.0 | Two-party margin in points each targeted district should clear: D seats aim for 50% + P/2, R seats for 50% - P/2. |
| `--seeds S` | cores, max 16 | Chains run in parallel per request; the best is kept. A request hits if its best chain hits. |
| `--seed X` | 1 | Seed of chain 0; chain i uses X + i. Runs are reproducible. |
| `--election 2024\|2020` | 2024 | Presidential race used for votes. California falls back to 2020 and says so in the output and tables. |
| `--scale F` | 1.0 | Multiplies the step counts. |

A run **hits** when the D seats won (D share above 50%) equal the request. A plan **passes** when it has the
apportioned number of districts, every district within 1% of the ideal population, every district contiguous, every
tract assigned once, and the population equal to the imported total. At-large states (AK, DE, ND, SD, VT, WY) are one
district without an anneal.

## GUI

```powershell
.\gradlew.bat :gui:run
```

Pick an imported state (or import with *Import RDH files…*), set the Democratic seats (the spinner shows 0..k and the
proportional default), the minimum margin, the seeds and the election, and press Run. The run happens in the
background with live phase, flips per second, score and current D–R; Cancel stops every chain. The map supports zoom
(scroll), pan (drag) and fit (double-click). The table lists each district's population, deviation, D share, winner
and margin. Export writes the PNG, GeoJSON or assignment CSV.

For checks without a person at the screen, snapshot mode runs a plan in the real window and saves it:

```powershell
gui\build\install\autoredistrict-gui\bin\autoredistrict-gui.bat --snapshot ME --dem-seats 1 --out out\gui\me.png
```

`.\gradlew.bat :gui:aotCache` builds an AOT cache (JEP 483/514/515) with a training run of that snapshot; the start
script uses it when present. On the development machine the window appears in about 0.73 s with the cache and
1.4 s without.

## Layout

| Module | Contents |
|---|---|
| `engine` | Pure Java, no I/O: the `Graph`, the chain state (`Districting`), the single-flip move kernel behind `MoveKernel`, seat-band scoring, the three-phase `Chain` and the parallel `Annealer`. |
| `data` | RDH import (GeoTools), the per-state Albers equal-area projection, adjacency, connectivity repair, block votes to tracts, the cache, the 2020 apportionment table. |
| `service` | Plan requests, the graph-Voronoi start, schedules, metrics and PASS checks, the batch, seat-range and artifacts. |
| `render` | Java2D PNGs, the contact sheet, GeoJSON, shapefile and CSV export; the map painter is shared with the GUI. |
| `cli` | picocli front end. |
| `gui` | JavaFX front end. |

The engine's hot loop runs on primitive arrays and allocates nothing. JDK 28 features in use: value classes
(JEP 401, preview) for small immutable types such as points, bounding boxes, edges, weights, targets and result rows;
structured concurrency for parallel chains, on platform threads because the work is CPU bound; lazy constants for a
few tables; ZGC with adaptive heap sizing (`-XX:+UseZGC -XX:+ZAdaptiveHeapSizing`, no `-Xmx`); and the AOT cache.

## Limits

- Seat results depend on one election's presidential vote, split from precincts to blocks by RDH.
- Some requests can't be met: a state's voters may be spread too evenly, or be too one-sided, for the requested count
  (Massachusetts Republicans are the usual example). The batch and seat-range report misses rather than failing.
- Optimality isn't guaranteed. Different seeds give different but similar maps.
