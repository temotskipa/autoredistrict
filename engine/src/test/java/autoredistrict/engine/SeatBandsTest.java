package autoredistrict.engine;

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
    void replaceTwoMatchesSorting() {
        SplittableRandom rng = new SplittableRandom(1);
        for (int t = 0; t < 2000; t++) {
            int k = 2 + rng.nextInt(20);
            double[] s = new double[k];
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
            double[] out = new double[k];
            SeatBands.replaceTwo(s, k, s[a], s[b], a1, b1, out);
            assertArrayEquals(expected, out, 0.0);
        }
    }
}
