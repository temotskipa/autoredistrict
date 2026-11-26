package com.temotskipa.autoredistrict.engine;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.*;

class SeatBandsTest {
    
    @Test
    void zeroExactlyWhenEverySortedDistrictIsInsideItsBand() {
        // 2 D seats, margin 2 points: D seats need >= 0.51, R seats <= 0.49.
        assertEquals(0.0, SeatBands.penalty(new double[]{0.30, 0.62, 0.45, 0.51}, 2, 2.0), 0.0);
        assertEquals(0.0, SeatBands.penalty(new double[]{0.49, 0.51}, 1, 2.0), 0.0);
        assertTrue(SeatBands.penalty(new double[]{0.30, 0.62, 0.45, 0.509}, 2, 2.0) > 0);
        assertTrue(SeatBands.penalty(new double[]{0.30, 0.62, 0.491, 0.55}, 2, 2.0) > 0);
        // 0 and k D seats.
        assertEquals(0.0, SeatBands.penalty(new double[]{0.2, 0.4, 0.49}, 0, 2.0), 0.0);
        assertEquals(0.0, SeatBands.penalty(new double[]{0.51, 0.7}, 2, 2.0), 0.0);
    }
    
    @Test
    void growsWithTheSquaredDistanceOutsideTheBand() {
        double one = SeatBands.penalty(new double[]{0.40, 0.50}, 1, 2.0);   // D seat 1 point short
        double three = SeatBands.penalty(new double[]{0.40, 0.48}, 1, 2.0); // D seat 3 points short
        assertEquals(1.0, one, 1e-9);
        assertEquals(9.0, three, 1e-9);
        double both = SeatBands.penalty(new double[]{0.50, 0.50}, 1, 2.0);  // one short, one over
        assertEquals(2.0, both, 1e-9);
        double prev = 0;
        for (double s = 0.51; s >= 0.30; s -= 0.01) {
            double p = SeatBands.penalty(new double[]{0.2, s}, 1, 2.0);
            assertTrue(p >= prev);
            prev = p;
        }
    }
    
    @Test
    void sortedAssignmentTakesTheTopSharesAsDemSeats() {
        // With 1 D seat the 0.60 district is the D seat; 0.52 must come down to 0.49.
        assertEquals(9.0, SeatBands.penalty(new double[]{0.52, 0.60, 0.30}, 1, 2.0), 1e-9);
    }
    
    @Test
    void spreadIsZeroWhenEachSidesSeatsAreEqual() {
        assertEquals(0.0, SeatBands.spread(new double[]{0.30, 0.30, 0.60, 0.60}, 2), 1e-9);
        assertEquals(0.0, SeatBands.spread(new double[]{0.65, 0.65, 0.65}, 3), 1e-9);
        // A side of one seat has nothing to spread.
        assertEquals(0.0, SeatBands.spread(new double[]{0.20, 0.70}, 1), 1e-9);
    }
    
    @Test
    void spreadMeasuresTheWeakSeatsOfEachSideFromItsOwnMean() {
        // R side 30 and 40 (mean 35): the 40 is the weaker R seat. D side 60 and 70 (mean 65):
        // the 60 is the weaker D seat. Each is 5 points off; the safer seats don't count.
        assertEquals(50.0, SeatBands.spread(new double[]{0.70, 0.30, 0.60, 0.40}, 2), 1e-9);
    }
    
    @Test
    void aPackedSeatCountsOnlyThroughTheSeatsItLeavesWeak() {
        // Same mean (60) on the D side: one packed seat and two equal ones 5 points under it,
        // against two equal seats and one marginal seat 10 points under.
        double packed = SeatBands.spread(new double[]{0.55, 0.55, 0.70}, 3);
        double marginal = SeatBands.spread(new double[]{0.50, 0.65, 0.65}, 3);
        assertEquals(50.0, packed, 1e-9);
        assertEquals(100.0, marginal, 1e-9);
    }
    
    @Test
    void evenlySafeSeatsScoreBetterThanPackedAndMarginalOnes() {
        // Three D seats, every one inside its band either way, with the same average.
        double[] packed = {0.52, 0.54, 0.89};
        double[] even = {0.65, 0.65, 0.65};
        double hi = SeatBands.hi(2.0);
        double lo = SeatBands.lo(2.0);
        assertEquals(0.0, SeatBands.penaltySorted(packed, 3, 3, hi, lo), 0.0);
        assertTrue(SeatBands.termSorted(even, 3, 3, hi, lo) < SeatBands.termSorted(packed, 3, 3, hi, lo));
    }
    
    @Test
    void replaceTwoMatchesSorting() {
        var rng = new SplittableRandom(1);
        for (int t = 0; t < 2000; t++) {
            int k = 2 + rng.nextInt(20);
            var s = new double[k];
            for (int i = 0; i < k; i++) {
                s[i] = rng.nextInt(10) / 10.0; // ties on purpose
            }
            Arrays.sort(s);
            int a = rng.nextInt(k);
            int b = (a + 1 + rng.nextInt(k - 1)) % k;
            double a1 = rng.nextDouble();
            double b1 = rng.nextDouble();
            double[] expected = s.clone();
            expected[a] = a1;
            expected[b] = b1;
            Arrays.sort(expected);
            var out = new double[k];
            SeatBands.replaceTwo(s, k, s[a], s[b], a1, b1, out);
            assertArrayEquals(expected, out, 0.0);
        }
    }
}
