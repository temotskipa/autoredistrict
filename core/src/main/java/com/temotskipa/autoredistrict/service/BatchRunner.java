package com.temotskipa.autoredistrict.service;

import com.temotskipa.autoredistrict.data.BlockVotes.Election;
import com.temotskipa.autoredistrict.data.StateDataset;
import com.temotskipa.autoredistrict.data.StateInfo;
import com.temotskipa.autoredistrict.engine.Progress;
import com.temotskipa.autoredistrict.render.MapRenderer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

/**
 * The all-states batch: for each state in the cache, run the proportional default, write its map
 * and report, and record a summary row. A state that fails is recorded and the batch goes on.
 * A seat miss doesn't fail a row; some states' geography can't produce the proportional count.
 */
public final class BatchRunner {
    
    /**
     * States must finish within this to PASS.
     */
    public static final double STATE_LIMIT_SECONDS = 15 * 60;
    
    private BatchRunner() {
    }
    
    public static Row row(PlanResult r) {
        PlanMetrics m = r.metrics();
        boolean pass = r.pass() && r.seconds() < STATE_LIMIT_SECONDS;
        Outcome outcome = pass ? new Passed() : new Failed(reasons(r));
        return new Row(r.state().abbr(), r.state().fips(), r.districts(), r.assignment().length, m.population(), m.maxDeviation() * 100, m.allContiguous(), m.assignedOnce(), m.populationMatches(), r.demRequested(), m.demWon(), m.repWon(), r.hit(), m.closestMargin(), r.flipsPerSecond(), r.seconds(), r.voteData(), outcome);
    }
    
    private static List<String> reasons(PlanResult r) {
        double tolerance = r.tolerance();
        PlanMetrics m = r.metrics();
        List<String> why = new ArrayList<>();
        if (m.districts() != r.state().seats()) {
            why.add("seats");
        }
        if (m.maxDeviation() > tolerance + 1e-12) {
            why.add("deviation");
        }
        if (!m.allContiguous()) {
            why.add("contiguity");
        }
        if (!m.assignedOnce()) {
            why.add("assignment");
        }
        if (!m.populationMatches()) {
            why.add("population");
        }
        if (r.seconds() >= STATE_LIMIT_SECONDS) {
            why.add("time");
        }
        return why;
    }
    
    public static Summary run(PlanService service, List<StateInfo> states, PlanOptions options, Election election, Path outDir, Consumer<String> log) throws InterruptedException {
        long t0 = System.nanoTime();
        List<Row> rows = new ArrayList<>();
        List<MapRenderer.Tile> tiles = new ArrayList<>();
        int seats = 0;
        for (StateInfo st : states) {
            long s0 = System.nanoTime();
            try {
                StateDataset ds = service.load(st, election);
                PlanResult r = service.run(ds, options, Progress.Listener.NONE);
                Artifacts.write(r, ds, outDir.resolve("states"), st.key(), false);
                Row row = row(r);
                rows.add(row);
                seats += r.districts();
                String caption = String.format(Locale.ROOT, "%s %d: %s%s", st.abbr(), r.districts(), MapRenderer.seats(r.metrics().demWon(), r.metrics().repWon()), r.hit() ? "" : " (miss)");
                tiles.add(new MapRenderer.Tile(caption, MapRenderer.thumbnail(Artifacts.mapData(r, ds), caption, 300, 230)));
                log.accept(String.format(Locale.ROOT, "%s: %d districts, requested %d D, won %d D, %s, max dev %.3f%%, " + "%.1f s, %,.0f flips/s, %s", st.abbr(), r.districts(), r.demRequested(), r.metrics().demWon(), r.hit() ? "hit" : "miss", r.metrics().maxDeviation() * 100, r.seconds(), r.flipsPerSecond(), row.status()));
            } catch (RuntimeException e) {
                double secs = (System.nanoTime() - s0) / 1e9;
                rows.add(new Row(st.abbr(), st.fips(), st.seats(), 0, 0, 0, false, false, false, 0, 0, 0, false, 0, 0, secs, "-", new Errored(e.toString())));
                log.accept(st.abbr() + ": ERROR " + e);
            }
        }
        var summary = new Summary(rows, (System.nanoTime() - t0) / 1e9, seats);
        write(summary, outDir);
        if (!tiles.isEmpty()) {
            MapRenderer.writePng(MapRenderer.contactSheet(String.format(Locale.ROOT, "autoredistrict — %d states, %d districts, proportional D seat targets (%d hit)", rows.size(), seats, summary.hitCount()), tiles, 10), outDir.resolve("contact-sheet.png"));
        }
        return summary;
    }
    
    public static void write(Summary s, Path dir) {
        try {
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("summary.csv"), csv(s), StandardCharsets.UTF_8);
            Files.writeString(dir.resolve("summary.md"), markdown(s), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write the batch summary in " + dir, e);
        }
    }
    
    public static String csv(Summary s) {
        var sb = new StringBuilder("""
                                   state,fips,seats,tracts,population,max_deviation_pct,all_contiguous,assigned_once,population_matches,\
                                   requested_d,won_d,won_r,hit,closest_margin_pts,flips_per_s,runtime_s,vote_data,status
                                   """);
        for (Row r : s.rows()) {
            sb.append(String.format(Locale.ROOT, "%s,%s,%d,%d,%d,%.4f,%b,%b,%b,%d,%d,%d,%b,%.3f,%.0f,%.1f,%s,\"%s\"%n", r.abbr(), r.fips(), r.seats(), r.tracts(), r.population(), r.maxDeviationPct(), r.contiguous(), r.assignedOnce(), r.populationMatches(), r.demRequested(), r.demWon(), r.repWon(), r.hit(), r.closestMargin(), r.flipsPerSecond(), r.seconds(), r.voteData(), r.status().replace("\"", "'")));
        }
        return sb.toString();
    }
    
    public static String markdown(Summary s) {
        var sb = new StringBuilder("""
                                   | # | ST | FIPS | seats | tracts | population | max dev % | contiguous | assigned once | pop matches \
                                   | req D | won D–R | hit | closest margin | flips/s | runtime s | vote data | status |
                                   |---:|---|---|---:|---:|---:|---:|---|---|---|---:|---|---|---:|---:|---:|---|---|
                                   """);
        int i = 0;
        for (Row r : s.rows()) {
            sb.append(String.format(Locale.ROOT, "| %d | %s | %s | %d | %,d | %,d | %.3f | %s | %s | %s | %d | %d–%d | %s | %.2f | %,.0f | %.1f | %s | %s |%n", ++i, r.abbr(), r.fips(), r.seats(), r.tracts(), r.population(), r.maxDeviationPct(), yes(r.contiguous()), yes(r.assignedOnce()), yes(r.populationMatches()), r.demRequested(), r.demWon(), r.repWon(), r.hit() ? "yes" : "no", r.closestMargin(), r.flipsPerSecond(), r.seconds(), r.voteData(), r.status()));
        }
        Row slow = s.slowest();
        sb.append(String.format(Locale.ROOT, "%n%d states, %d seats in total; %d PASS; %d of %d hit their requested D " + "seats.%nTotal wall time %.1f s (%.1f min); slowest state %s at %.1f s.%n", s.rows().size(), s.totalSeats(), s.passCount(), s.hitCount(), s.rows().size(), s.wallSeconds(), s.wallSeconds() / 60, slow == null ? "-" : slow.abbr(), slow == null ? 0 : slow.seconds()));
        return sb.toString();
    }
    
    private static String yes(boolean b) {
        return b ? "yes" : "NO";
    }
    
    /**
     * How one state's run ended.
     */
    public sealed interface Outcome permits Passed, Failed, Errored {
    }
    
    /**
     * The plan meets every PASS rule and finished within {@link #STATE_LIMIT_SECONDS}.
     */
    public value record Passed() implements Outcome {
    }
    
    /**
     * The plan broke the PASS rules named in {@code reasons} (seats, deviation, contiguity,
     * assignment, population, time).
     */
    public value record Failed(List<String> reasons) implements Outcome {
        
        public Failed {
            reasons = List.copyOf(reasons);
        }
    }
    
    /**
     * The state couldn't be run, for the reason in {@code message}; it has no plan or map.
     */
    public value record Errored(String message) implements Outcome {
    }
    
    /**
     * One summary row.
     */
    public value record Row(String abbr, String fips, int seats, int tracts, long population, double maxDeviationPct, boolean contiguous, boolean assignedOnce, boolean populationMatches, int demRequested, int demWon, int repWon, boolean hit, double closestMargin, double flipsPerSecond, double seconds, String voteData, Outcome outcome) {
        
        public boolean pass() {
            return outcome instanceof Passed;
        }
        
        /**
         * PASS, FAIL with its reasons, or ERROR with the message, as the reports show it.
         */
        public String status() {
            return switch (outcome) {
                case Passed() -> "PASS";
                case Failed(var reasons) when reasons.isEmpty() -> "FAIL";
                case Failed(var reasons) -> "FAIL (" + String.join(", ", reasons) + ")";
                case Errored(var message) -> "ERROR: " + message;
            };
        }
    }
    
    /**
     * The batch as a whole.
     */
    public value record Summary(List<Row> rows, double wallSeconds, int totalSeats) {
        
        public long passCount() {
            return rows.stream().filter(Row::pass).count();
        }
        
        public long hitCount() {
            return rows.stream().filter(Row::hit).count();
        }
        
        public Row slowest() {
            return rows.stream().max(Comparator.comparingDouble(Row::seconds)).orElse(null);
        }
        
        /**
         * Whether any state drew a map, so the contact sheet was written.
         */
        public boolean drewMaps() {
            return rows.stream().anyMatch(r -> !(r.outcome() instanceof Errored));
        }
    }
}
