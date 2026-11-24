package com.temotskipa.autoredistrict.engine;

import java.util.Arrays;

/**
 * The election-fit term, adapted from the video's "desired results" curve to a seat target.
 * Districts are sorted by D share; the top {@code demSeats} are D seats and must reach
 * {@code 50% + margin/2}, the rest are R seats and must stay at or below {@code 50% - margin/2}.
 * The penalty is the sum of squared distances outside those bands, in percentage points, and is
 * zero exactly when every district sits inside its band. Districts near 50% are the ones outside
 * the bands, so they carry the weight, like the video's swing-seat weighting.
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
     * Penalty for shares sorted ascending (the last {@code demSeats} are the D seats).
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
     * Penalty for shares in any order.
     */
    public static double penalty(double[] shares, int demSeats, double marginPoints) {
        double[] sorted = shares.clone();
        Arrays.sort(sorted);
        return penaltySorted(sorted, sorted.length, demSeats, hi(marginPoints), lo(marginPoints));
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
