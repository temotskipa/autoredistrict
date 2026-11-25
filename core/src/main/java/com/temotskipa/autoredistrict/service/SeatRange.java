package com.temotskipa.autoredistrict.service;

import java.util.*;

/**
 * The seat-range command: request the proportional count first, then step outward one seat at a
 * time in each direction until a request misses, and stop that direction. A request only counts
 * as a hit if its plan also meets the PASS rules.
 */
public final class SeatRange {
    
    private SeatRange() {
    }
    
    public static List<Row> run(int k, int proportional, Requester requester) throws InterruptedException {
        List<Row> rows = new ArrayList<>();
        rows.add(requester.request(proportional));
        for (int n = proportional + 1; n <= k; n++) {
            Row r = requester.request(n);
            rows.add(r);
            if (!r.counts()) {
                break;
            }
        }
        for (int n = proportional - 1; n >= 0; n--) {
            Row r = requester.request(n);
            rows.add(r);
            if (!r.counts()) {
                break;
            }
        }
        return rows;
    }
    
    /**
     * The distinct D seat counts that were hit (and passed).
     */
    public static Set<Integer> distinctHits(List<Row> rows) {
        Set<Integer> s = new TreeSet<>();
        for (Row r : rows) {
            if (r.counts()) {
                s.add(r.demWon());
            }
        }
        return s;
    }
    
    public static Row rowOf(PlanResult r) {
        return new Row(r.demRequested(), r.metrics().demWon(), r.metrics().repWon(), r.hit(), r.metrics().closestMargin(), r.metrics().maxDeviation(), r.metrics().allContiguous(), r.pass(), r.voteData(), r.seeds(), r.seconds(), r.flipsPerSecond());
    }
    
    /**
     * Markdown table, rows sorted by requested seats.
     */
    public static String table(String state, int k, List<Row> rows) {
        List<Row> sorted = new ArrayList<>(rows);
        sorted.sort(java.util.Comparator.comparingInt(Row::requested));
        StringBuilder sb = new StringBuilder();
        sb.append("| ").append(state).append(" requested D | won D–R | hit | closest margin (pts) | max dev % " + "| contiguous | PASS | vote data | seeds | runtime s | flips/s |\n");
        sb.append("|---:|---|---|---:|---:|---|---|---|---:|---:|---:|\n");
        for (Row r : sorted) {
            sb.append(String.format(Locale.ROOT, "| %d | %d–%d | %s | %.2f | %.3f | %s | %s | %s | %d | %.1f | %,.0f |%n", r.requested(), r.demWon(), r.repWon(), r.counts() ? "yes" : (r.hit() ? "hit, not PASS" : "no"), r.closestMargin(), r.maxDeviation() * 100, r.contiguous() ? "yes" : "NO", r.pass() ? "PASS" : "FAIL", r.voteData(), r.seeds(), r.seconds(), r.flipsPerSecond()));
        }
        Set<Integer> hits = distinctHits(rows);
        sb.append(String.format(Locale.ROOT, "%n%s (%d seats): %d distinct D seat counts hit: %s%n", state, k, hits.size(), hits));
        return sb.toString();
    }
    
    /**
     * Runs one request; the real one runs a plan, tests use a fake.
     */
    @FunctionalInterface
    public interface Requester {
        Row request(int demSeats) throws InterruptedException;
    }
    
    /**
     * One request of a seat range. A value object.
     */
    public value record Row(int requested, int demWon, int repWon, boolean hit, double closestMargin, double maxDeviation, boolean contiguous, boolean pass, String voteData, int seeds, double seconds, double flipsPerSecond) {
        
        /**
         * A hit that also meets the PASS rules.
         */
        public boolean counts() {
            return hit && pass;
        }
    }
}
