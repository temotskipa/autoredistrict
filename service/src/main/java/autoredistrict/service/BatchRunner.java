package autoredistrict.service;

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

import autoredistrict.data.BlockVotes.Election;
import autoredistrict.data.StateDataset;
import autoredistrict.data.StateInfo;
import autoredistrict.engine.Progress;
import autoredistrict.render.MapRenderer;

/**
 * The all-states batch: for each state in the cache, run the proportional default, write its map
 * and report, and record a summary row. A state that fails is recorded and the batch goes on.
 * A seat miss doesn't fail a row; some states' geography can't produce the proportional count.
 */
public final class BatchRunner {

    /** States must finish within this to PASS. */
    public static final double STATE_LIMIT_SECONDS = 15 * 60;

    private BatchRunner() {
    }

    /** One summary row. */
    public record Row(String abbr, String fips, int seats, int tracts, long population, double maxDeviationPct,
                      boolean contiguous, boolean assignedOnce, boolean populationMatches, int demRequested,
                      int demWon, int repWon, boolean hit, double closestMargin, double flipsPerSecond,
                      double seconds, String voteData, String status, String error) {

        public boolean pass() {
            return status.equals("PASS");
        }
    }

    /** The batch as a whole. */
    public record Summary(List<Row> rows, double wallSeconds, int totalSeats) {

        public long passCount() {
            return rows.stream().filter(Row::pass).count();
        }

        public long hitCount() {
            return rows.stream().filter(Row::hit).count();
        }

        public Row slowest() {
            return rows.stream().max(Comparator.comparingDouble(Row::seconds)).orElse(null);
        }
    }

    public static Row row(PlanResult r, double tolerance) {
        PlanMetrics m = r.metrics();
        boolean pass = r.pass(tolerance) && r.seconds() < STATE_LIMIT_SECONDS;
        String why = pass ? "PASS" : "FAIL" + reasons(r, tolerance);
        return new Row(r.state().abbr(), r.state().fips(), r.districts(), r.assignment().length, m.population(),
                m.maxDeviation() * 100, m.allContiguous(), m.assignedOnce(), m.populationMatches(), r.demRequested(),
                m.demWon(), m.repWon(), r.hit(), m.closestMargin(), r.flipsPerSecond(), r.seconds(), r.voteData(), why,
                null);
    }

    private static String reasons(PlanResult r, double tolerance) {
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
        return why.isEmpty() ? "" : " (" + String.join(", ", why) + ")";
    }

    public static Summary run(PlanService service, List<StateInfo> states, PlanOptions options, Election election,
                              Path outDir, Consumer<String> log) throws InterruptedException {
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
                Row row = row(r, options.tolerance());
                rows.add(row);
                seats += r.districts();
                String caption = String.format(Locale.ROOT, "%s %d: %s%s", st.abbr(), r.districts(),
                        MapRenderer.seats(r.metrics().demWon(), r.metrics().repWon()), r.hit() ? "" : " (miss)");
                tiles.add(new MapRenderer.Tile(caption, MapRenderer.thumbnail(Artifacts.mapData(r, ds), caption, 300, 230)));
                log.accept(String.format(Locale.ROOT, "%s: %d districts, requested %d D, won %d D, %s, max dev %.3f%%, "
                                + "%.1f s, %,.0f flips/s, %s", st.abbr(), r.districts(), r.demRequested(), r.metrics().demWon(),
                        r.hit() ? "hit" : "miss", r.metrics().maxDeviation() * 100, r.seconds(), r.flipsPerSecond(),
                        row.status()));
            } catch (InterruptedException e) {
                throw e;
            } catch (RuntimeException e) {
                double secs = (System.nanoTime() - s0) / 1e9;
                rows.add(new Row(st.abbr(), st.fips(), st.seats(), 0, 0, 0, false, false, false, 0, 0, 0, false, 0, 0,
                        secs, "-", "ERROR", e.toString()));
                log.accept(st.abbr() + ": ERROR " + e);
            }
        }
        Summary summary = new Summary(rows, (System.nanoTime() - t0) / 1e9, seats);
        write(summary, outDir);
        if (!tiles.isEmpty()) {
            MapRenderer.writePng(MapRenderer.contactSheet(String.format(Locale.ROOT,
                    "autoredistrict — %d states, %d districts, proportional D seat targets (%d hit)", rows.size(),
                    seats, summary.hitCount()), tiles, 10), outDir.resolve("contact-sheet.png"));
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
        StringBuilder sb = new StringBuilder("state,fips,seats,tracts,population,max_deviation_pct,all_contiguous,"
                + "assigned_once,population_matches,requested_d,won_d,won_r,hit,closest_margin_pts,flips_per_s,"
                + "runtime_s,vote_data,status\n");
        for (Row r : s.rows()) {
            sb.append(String.format(Locale.ROOT, "%s,%s,%d,%d,%d,%.4f,%b,%b,%b,%d,%d,%d,%b,%.3f,%.0f,%.1f,%s,\"%s\"%n",
                    r.abbr(), r.fips(), r.seats(), r.tracts(), r.population(), r.maxDeviationPct(), r.contiguous(),
                    r.assignedOnce(), r.populationMatches(), r.demRequested(), r.demWon(), r.repWon(), r.hit(),
                    r.closestMargin(), r.flipsPerSecond(), r.seconds(), r.voteData(),
                    r.error() == null ? r.status() : r.status() + ": " + r.error().replace("\"", "'")));
        }
        return sb.toString();
    }

    public static String markdown(Summary s) {
        StringBuilder sb = new StringBuilder();
        sb.append("| # | ST | FIPS | seats | tracts | population | max dev % | contiguous | assigned once | pop matches "
                + "| req D | won D–R | hit | closest margin | flips/s | runtime s | vote data | status |\n");
        sb.append("|---:|---|---|---:|---:|---:|---:|---|---|---|---:|---|---|---:|---:|---:|---|---|\n");
        int i = 0;
        for (Row r : s.rows()) {
            sb.append(String.format(Locale.ROOT,
                    "| %d | %s | %s | %d | %,d | %,d | %.3f | %s | %s | %s | %d | %d–%d | %s | %.2f | %,.0f | %.1f | %s | %s |%n",
                    ++i, r.abbr(), r.fips(), r.seats(), r.tracts(), r.population(), r.maxDeviationPct(), yes(r.contiguous()),
                    yes(r.assignedOnce()), yes(r.populationMatches()), r.demRequested(), r.demWon(), r.repWon(),
                    r.hit() ? "yes" : "no", r.closestMargin(), r.flipsPerSecond(), r.seconds(), r.voteData(),
                    r.error() == null ? r.status() : r.status() + ": " + r.error()));
        }
        Row slow = s.slowest();
        sb.append(String.format(Locale.ROOT, "%n%d states, %d seats in total; %d PASS; %d of %d hit their requested D "
                        + "seats.%nTotal wall time %.1f s (%.1f min); slowest state %s at %.1f s.%n",
                s.rows().size(), s.totalSeats(), s.passCount(), s.hitCount(), s.rows().size(), s.wallSeconds(),
                s.wallSeconds() / 60, slow == null ? "-" : slow.abbr(), slow == null ? 0 : slow.seconds()));
        return sb.toString();
    }

    private static String yes(boolean b) {
        return b ? "yes" : "NO";
    }
}
