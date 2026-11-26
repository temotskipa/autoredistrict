package com.temotskipa.autoredistrict.cli;

import com.temotskipa.autoredistrict.data.BlockVotes.Election;
import com.temotskipa.autoredistrict.data.*;
import com.temotskipa.autoredistrict.engine.Annealer;
import com.temotskipa.autoredistrict.engine.Progress;
import com.temotskipa.autoredistrict.service.*;
import picocli.CommandLine;
import picocli.CommandLine.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The autoredistrict command line. Commands are thin: all work happens in the service module.
 */
@Command(name = "autoredistrict", mixinStandardHelpOptions = true, version = "autoredistrict 2.0", description = "Seat-targeted congressional maps from Redistricting Data Hub tract data.", subcommands = {Main.ImportCommand.class, Main.RunCommand.class, Main.AllCommand.class, Main.SeatRangeCommand.class})
public final class Main implements Runnable {
    
    @Option(names = "--cache-dir", description = "Cache folder (default: .cache).", defaultValue = ".cache", scope = ScopeType.INHERIT)
    Path cacheDir;
    
    void main(String[] args) {
        System.exit(run(args));
    }
    
    public static int run(String... args) {
        var cl = new CommandLine(new Main());
        cl.setExecutionExceptionHandler((ex, cmd, _) -> switch (ex) {
            case DataException _, IllegalArgumentException _ -> {
                cmd.getErr().println("error: " + ex.getMessage());
                yield 2;
            }
            default -> throw ex;
        });
        return cl.execute(args);
    }
    
    /**
     * Parses "all" (every state with districts) or a comma-separated list; a state named twice
     * (NC,nc,37) is kept once. A list that names no state (an empty value, or only commas) or names
     * an at-large state is an error.
     */
    static List<StateInfo> parseStates(String spec) {
        if (spec == null || spec.equalsIgnoreCase("all")) {
            return States.districted();
        }
        Set<StateInfo> out = new LinkedHashSet<>();
        for (String s : spec.split(",")) {
            if (!s.isBlank()) {
                out.add(States.require(s.strip()).requireDistricts());
            }
        }
        if (out.isEmpty()) {
            throw new DataException("--states names no state. Give all, or a comma-separated list such as ME,NC,37.");
        }
        return List.copyOf(out);
    }
    
    static void writeText(Path file, String text) {
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
            Files.writeString(file, text, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write " + file, e);
        }
    }
    
    /**
     * Prints a progress line now and then (every few seconds) from chain 0.
     */
    static Progress.Listener consoleProgress(boolean quiet) {
        if (quiet) {
            return Progress.Listener.NONE;
        }
        var last = new AtomicLong();
        return p -> {
            long now = System.nanoTime();
            long prev = last.get();
            if (p.chain() == 0 && now - prev > 5_000_000_000L && last.compareAndSet(prev, now)) {
                System.out.printf(Locale.ROOT, "  [%s %d/%d] step %,d/%,d  score %.4f  D won %d  max dev %.2f%%  %,.0f flips/s%n", p.phase(), p.phaseIndex() + 1, p.phaseCount(), p.step(), p.phaseSteps(), p.score(), p.demWon(), p.maxDeviation() * 100, p.flipsPerSecond());
            }
        };
    }
    
    static void printResult(PlanResult r) {
        System.out.printf(Locale.ROOT, "%s (%s): %d districts, requested %d D%s, won %d D–%d R: %s%n", r.state().name(), r.state().abbr(), r.districts(), r.demRequested(), r.proportionalDefault() ? " (proportional)" : "", r.metrics().demWon(), r.metrics().repWon(), r.hit() ? "HIT" : "MISS");
        System.out.printf(Locale.ROOT, "  max deviation %.3f%%, contiguous %s, assigned once %s, population %,d = %,d: %s; " + "closest margin %.2f pts%n", r.metrics().maxDeviation() * 100, r.metrics().allContiguous(), r.metrics().assignedOnce(), r.metrics().population(), r.metrics().importedPopulation(), r.metrics().populationMatches(), r.metrics().closestMargin());
        System.out.printf(Locale.ROOT, "  votes %s (statewide D share %.4f); %d seeds, %d hit; %,d flips in %.1f s = %,.0f flips/s%n", r.voteData(), r.statewideDemShare(), r.seeds(), r.chainsHit(), r.proposals(), r.seconds(), r.flipsPerSecond());
        System.out.println("  schedule: " + r.schedule());
        r.warnings().forEach(w -> System.out.println("  warning: " + w));
        System.out.println("  PASS: " + (r.pass() ? "yes" : "NO"));
    }
    
    @Override
    public void run() {
        new CommandLine(this).usage(System.out);
    }
    
    /**
     * Options shared by run, all and seat-range.
     */
    static final class PlanArgs {
        
        @Option(names = "--min-margin", defaultValue = "2.0", description = "Two-party margin in points each targeted district should clear (default 2.0).")
        double minMargin;
        
        @Option(names = "--seeds", description = "Chains per request, run in parallel, 1 to 256 (default: cores, max 16).")
        Integer seeds;
        
        @Option(names = "--seed", defaultValue = "1", description = "Seed of the first chain (default 1).")
        long seed;
        
        @Option(names = "--election", defaultValue = "2024", description = "Presidential race for votes: 2024 (default) or 2020. CA falls back to 2020.")
        String election;
        
        @Option(names = "--scale", defaultValue = "1.0", description = "Multiplies the anneal's step counts.")
        double scale;
        
        @Option(names = "--budget-min", defaultValue = "12", description = "Per-chain time budget in minutes.")
        long budgetMinutes;
        
        @Option(names = "--resolution", defaultValue = "tract", description = "Unit resolution; only tract.")
        String resolution;
        
        PlanOptions options(Integer demSeats) {
            if (!resolution.equalsIgnoreCase("tract")) {
                throw new DataException("Only --resolution tract is supported.");
            }
            return new PlanOptions(demSeats, minMargin, seeds == null ? Annealer.defaultSeeds() : seeds, seed, scale, Duration.ofMinutes(budgetMinutes), PlanOptions.DEFAULT_TOLERANCE);
        }
        
        Election election() {
            return Election.parse(election);
        }
    }
    
    @Command(name = "import", mixinStandardHelpOptions = true, description = "Import RDH tract zips and vote files from a download folder into the cache.")
    static final class ImportCommand implements Callable<Integer> {
        
        @ParentCommand
        Main parent;
        
        @Option(names = "--rdh-dir", required = true, description = "Folder with the RDH downloads.")
        Path rdhDir;
        
        @Option(names = "--states", defaultValue = "all", description = "Comma-separated states, or all (default): every state with districts.")
        String states;
        
        @Option(names = "--parallel", defaultValue = "6", description = "States imported at once (default 6).")
        int parallel;
        
        @Override
        public Integer call() throws Exception {
            List<StateInfo> list = parseStates(states);
            long t0 = System.nanoTime();
            List<StateImporter.ImportResult> rows;
            try {
                rows = ImportService.importFolder(rdhDir, list, parent.cacheDir, parallel, System.out::println);
            } catch (StateImporter.MissingFilesException e) {
                System.err.println("Missing RDH files in " + rdhDir + ". Download these from redistrictingdatahub.org:");
                e.files().forEach(f -> System.err.println("  " + f));
                return 3;
            }
            System.out.println();
            System.out.print(ImportService.summaryTable(rows));
            for (StateImporter.ImportResult r : rows) {
                if (r instanceof StateImporter.ImportSummary s) {
                    s.warnings().forEach(w -> System.out.println("warning " + s.state().abbr() + ": " + w));
                }
            }
            System.out.printf(Locale.ROOT, "%nImported %d of %d states in %.1f s.%n", rows.stream().filter(StateImporter.ImportResult::ok).count(), rows.size(), (System.nanoTime() - t0) / 1e9);
            return rows.stream().allMatch(StateImporter.ImportResult::ok) ? 0 : 1;
        }
    }
    
    /**
     * A seat request: Democratic or Republican seats, not both. Without either, the proportional
     * default.
     */
    static final class SeatRequest {
        
        @Option(names = "--dem-seats", description = "Democratic seats to deliver (default: proportional).")
        Integer dem;
        
        @Option(names = "--rep-seats", description = "Republican seats to deliver; Democrats get the rest.")
        Integer rep;
        
        /**
         * The Democratic seats this asks for in {@code st}.
         */
        Integer demSeats(StateInfo st) {
            if (rep == null) {
                return dem;
            }
            if (rep < 0 || rep > st.seats()) {
                throw new DataException("--rep-seats for " + st.abbr() + " must be between 0 and " + st.seats() + ", not " + rep + ".");
            }
            return st.seats() - rep;
        }
    }
    
    @Command(name = "run", mixinStandardHelpOptions = true, description = "Draw one state's map for a requested number of Democratic or Republican seats.")
    static final class RunCommand implements Callable<Integer> {
        
        @ParentCommand
        Main parent;
        
        @Parameters(index = "0", description = "State, e.g. NC.")
        String state;
        
        // An ArgGroup is exclusive by default: --dem-seats or --rep-seats, not both.
        @ArgGroup
        SeatRequest seats;
        
        @Option(names = "--out-dir", defaultValue = "out", description = "Where to write the map and reports.")
        Path outDir;
        
        @Option(names = "--quiet", description = "No progress lines.")
        boolean quiet;
        
        @Mixin
        PlanArgs args = new PlanArgs();
        
        @Override
        public Integer call() throws Exception {
            StateInfo st = States.require(state).requireDistricts();
            var svc = new PlanService(parent.cacheDir);
            StateDataset ds = svc.load(st, args.election());
            PlanResult r = svc.run(ds, args.options(seats == null ? null : seats.demSeats(st)), consoleProgress(quiet));
            printResult(r);
            System.out.print(Artifacts.districtTable(r));
            String stem = st.key() + "-" + r.demRequested() + "d";
            for (Path p : Artifacts.write(r, ds, outDir, stem, true)) {
                System.out.println("wrote " + p);
            }
            return r.pass() ? 0 : 1;
        }
    }
    
    @Command(name = "all", mixinStandardHelpOptions = true, description = "Run every imported state at its proportional default and write a summary and contact sheet.")
    static final class AllCommand implements Callable<Integer> {
        
        @ParentCommand
        Main parent;
        
        @Option(names = "--out-dir", defaultValue = "out/all", description = "Output folder.")
        Path outDir;
        
        @Option(names = "--states", defaultValue = "all", description = "Comma-separated states, or all (default): every state with districts.")
        String states;
        
        @Mixin
        PlanArgs args = new PlanArgs();
        
        @Override
        public Integer call() throws Exception {
            List<StateInfo> list = new ArrayList<>();
            for (StateInfo s : parseStates(states)) {
                if (TractCache.isImported(parent.cacheDir, s)) {
                    list.add(s);
                }
                else {
                    System.out.println("skipping " + s.abbr() + ": not imported");
                }
            }
            if (list.isEmpty()) {
                System.err.println("No imported states to run. Import them first: autoredistrict import --rdh-dir <folder>");
                return 1;
            }
            BatchRunner.Summary s = BatchRunner.run(new PlanService(parent.cacheDir), list, args.options(null), args.election(), outDir, System.out::println);
            System.out.println();
            System.out.print(BatchRunner.markdown(s));
            System.out.println("wrote " + outDir.resolve("summary.md") + ", summary.csv" + (s.drewMaps() ? ", contact-sheet.png" : ""));
            return s.passCount() == s.rows().size() ? 0 : 1;
        }
    }
    
    @Command(name = "seat-range", mixinStandardHelpOptions = true, description = "Request the proportional seat count, then step outward until a request misses.")
    static final class SeatRangeCommand implements Callable<Integer> {
        
        @ParentCommand
        Main parent;
        
        @Parameters(index = "0", description = "State, e.g. NC.")
        String state;
        
        @Option(names = "--out-dir", defaultValue = "out/seat-range", description = "Output folder.")
        Path outDir;
        
        @Mixin
        PlanArgs args = new PlanArgs();
        
        @Override
        public Integer call() throws Exception {
            StateInfo st = States.require(state).requireDistricts();
            var svc = new PlanService(parent.cacheDir);
            StateDataset ds = svc.load(st, args.election());
            int k = st.seats();
            int prop = PlanOptions.proportional(ds.demShare(), k);
            Path dir = outDir.resolve(st.key());
            List<SeatRange.Row> rows = SeatRange.run(k, prop, n -> {
                PlanResult r = svc.runUntilHit(ds, args.options(n), Progress.Listener.NONE);
                Artifacts.write(r, ds, dir, st.key() + "-" + n + "d", false);
                SeatRange.Row row = SeatRange.rowOf(r);
                System.out.printf(Locale.ROOT, "%s requested %d D: won %d D–%d R, %s, max dev %.3f%%, %.1f s%n", st.abbr(), n, row.demWon(), row.repWon(), row.counts() ? "HIT" : "miss", row.maxDeviation() * 100, row.seconds());
                return row;
            });
            String table = SeatRange.table(st.abbr(), k, rows);
            writeText(dir.resolve("seat-range.md"), table);
            System.out.println();
            System.out.print(table);
            return 0;
        }
    }
}
