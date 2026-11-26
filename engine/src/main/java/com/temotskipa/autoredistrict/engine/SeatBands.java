package com.temotskipa.autoredistrict.engine;

import java.util.Arrays;

/**
 * The election-fit term, adapted from the video's "desired results" curve to a seat target.
 * Districts are sorted by D share; the top {@code demSeats} are D seats and must reach
 * {@code 50% + margin/2}, the rest are R seats and must stay at or below {@code 50% - margin/2}.
 *
 * <p>The term has two parts, both in squared percentage points:
 * <ul>
 *   <li>the <b>band penalty</b>: squared distances outside those bands, zero exactly when every
 *       district sits inside its band. Districts near 50% are the ones outside the bands, so they
 *       carry the weight, like the video's swing-seat weighting;</li>
 *   <li>the <b>spread</b>: for each seat weaker than its side's mean share (a D seat below the D
 *       seats' mean, an R seat above the R seats' mean D share), the squared distance from that
 *       mean, weighted by {@link #SPREAD_WEIGHT}. It lifts a party's marginal seats toward its
 *       average seat, so the seats are about equally safe instead of a few packed seats and many
 *       near the edge of the band. Packed seats are not penalised themselves, only through the
 *       seats they leave weak, so the anneal gives up compactness to unpack them only where that
 *       makes another seat safer. The sides are taken by rank, and a side's mean is inside its
 *       band whenever every seat is, so the spread never pulls a seat out of its band.</li>
 * </ul>
 */
public final class SeatBands {
    
    private SeatBands() {
    }
    
    /**
     * Lower edge of the D band, as a share.
     */
    public static double hi(double marginPoints) {
        return 0.5 + marginPoints / 200.0;
    }
    
    /**
     * Upper edge of the R band, as a share.
     */
    public static double lo(double marginPoints) {
        return 0.5 - marginPoints / 200.0;
    }
    
    /**
     * Weight of the spread against the band penalty.
     */
    static final double SPREAD_WEIGHT = 0.2;
    
    /**
     * The whole term, band penalty plus weighted spread, for shares sorted ascending (the last
     * {@code demSeats} are the D seats).
     */
    public static double termSorted(double[] ascending, int k, int demSeats, double hi, double lo) {
        return penaltySorted(ascending, k, demSeats, hi, lo) + SPREAD_WEIGHT * spreadSorted(ascending, k, demSeats);
    }
    
    /**
     * Band penalty for shares sorted ascending (the last {@code demSeats} are the D seats).
     */
    public static double penaltySorted(double[] ascending, int k, int demSeats, double hi, double lo) {
        int firstD = k - demSeats;
        double pen = 0;
        for (int i = 0; i < k; i++) {
            double s = ascending[i];
            double excess = i >= firstD ? hi - s : s - lo;
            if (excess > 0) {
                excess *= 100.0;
                pen += excess * excess;
            }
        }
        return pen;
    }
    
    /**
     * Spread for shares sorted ascending: the sum, over both sides, of the squared distance from
     * the side's mean of each seat weaker than that mean, in percentage points.
     */
    public static double spreadSorted(double[] ascending, int k, int demSeats) {
        int firstD = k - demSeats;
        return sideSpread(ascending, 0, firstD, 1) + sideSpread(ascending, firstD, k, -1);
    }
    
    /**
     * The side's part of the spread. {@code weak} is the direction of a weaker seat's share from
     * the mean: -1 for D seats (lower D share), +1 for R seats (higher D share).
     */
    private static double sideSpread(double[] shares, int from, int to, int weak) {
        int n = to - from;
        if (n < 2) {
            return 0;
        }
        double sum = 0;
        for (int i = from; i < to; i++) {
            sum += shares[i];
        }
        double mean = sum / n;
        double spread = 0;
        for (int i = from; i < to; i++) {
            double d = (shares[i] - mean) * weak * 100.0;
            if (d > 0) {
                spread += d * d;
            }
        }
        return spread;
    }
    
    /**
     * Band penalty for shares in any order.
     */
    public static double penalty(double[] shares, int demSeats, double marginPoints) {
        double[] sorted = shares.clone();
        Arrays.sort(sorted);
        return penaltySorted(sorted, sorted.length, demSeats, hi(marginPoints), lo(marginPoints));
    }
    
    /**
     * Spread for shares in any order.
     */
    public static double spread(double[] shares, int demSeats) {
        double[] sorted = shares.clone();
        Arrays.sort(sorted);
        return spreadSorted(sorted, sorted.length, demSeats);
    }
    
    /**
     * Writes into {@code dst} the ascending array {@code src} with one instance each of {@code a0}
     * and {@code b0} removed and {@code a1}, {@code b1} inserted. O(k), allocation free.
     */
    static void replaceTwo(double[] src, int k, double a0, double b0, double a1, double b1, double[] dst) {
        double lo1 = Math.min(a1, b1);
        double hi1 = Math.max(a1, b1);
        boolean removedA = false;
        boolean removedB = false;
        boolean insLo = false;
        boolean insHi = false;
        int j = 0;
        for (int i = 0; i < k; i++) {
            double v = src[i];
            if (!removedA && v == a0) {
                removedA = true;
                continue;
            }
            if (!removedB && v == b0) {
                removedB = true;
                continue;
            }
            if (!insLo && lo1 <= v) {
                dst[j++] = lo1;
                insLo = true;
            }
            if (insLo && !insHi && hi1 <= v) {
                dst[j++] = hi1;
                insHi = true;
            }
            dst[j++] = v;
        }
        if (!insLo) {
            dst[j++] = lo1;
        }
        if (!insHi) {
            dst[j++] = hi1;
        }
        if (j != k || !removedA || !removedB) {
            throw new IllegalStateException("share bookkeeping out of sync");
        }
    }
}
