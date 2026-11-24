package autoredistrict.engine;

import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DistrictingTest {
    
    private static Graph mixedGrid() {
        return Grids.lattice(20, 20, 100, i -> 0.2 + 0.6 * ((i * 37) % 100) / 100.0);
    }
    
    @Test
    void incrementalTalliesMatchFullRecomputationAfterRandomFlips() {
        Graph g = mixedGrid();
        SplittableRandom rng = Grids.rng(7);
        Districting d = new Districting(g, 5, Grids.BFS_VORONOI.assign(g, 5, rng), 0.5);
        d.setTarget(2, 2.0);
        d.setWeights(new Weights(1, 10, 1, 1));
        MoveKernel.Stats stats = new MoveKernel.Stats();
        for (int i = 0; i < 50_000; i++) {
            SingleFlip.INSTANCE.step(d, rng, 1e9, 0.5, stats); // hot: nearly every legal flip is taken
            if (i % 5_000 == 0) {
                d.verifyTallies();
            }
        }
        assertTrue(stats.accepted > 10_000, "accepted " + stats.accepted);
        d.verifyTallies();
        long total = 0;
        for (int k = 0; k < 5; k++) {
            total += d.populationOf(k);
        }
        assertEquals(g.totalPop(), total, "no phantom voters");
    }
    
    @Test
    void contiguityCheckAgreesWithFullBfs() {
        Graph g = mixedGrid();
        SplittableRandom rng = Grids.rng(11);
        Districting d = new Districting(g, 6, Grids.BFS_VORONOI.assign(g, 6, rng), 10);
        d.setWeights(new Weights(0, 0, 0, 0));
        MoveKernel.Stats stats = new MoveKernel.Stats();
        int checked = 0;
        int splits = 0;
        for (int i = 0; i < 20_000; i++) {
            SingleFlip.INSTANCE.step(d, rng, 1e9, 0, stats);
            int u = d.pickBoundary(rng, 0);
            int[] a = d.assignment();
            boolean fast = d.removalKeepsConnected(u);
            boolean full = Grids.connectedWithout(g, a, a[u], u);
            assertEquals(full, fast, "unit " + u + " at step " + i);
            checked++;
            if (!full) {
                splits++;
            }
        }
        assertTrue(splits > 100, "the walk should produce cut vertices too: " + splits + " of " + checked);
        // Every district is still contiguous after the walk.
        int[] a = d.assignment();
        for (int k = 0; k < 6; k++) {
            assertTrue(Grids.connectedWithout(g, a, k, -1), "district " + k);
        }
    }
    
    @Test
    void populationBoundIsHardOnceADistrictIsInside() {
        // 10x10 grid split into two 5-column halves: both exactly at ideal (500 each of 1000).
        Graph g = Grids.lattice(10, 10, 10, i -> 0.5);
        int[] init = new int[100];
        for (int i = 0; i < 100; i++) {
            init[i] = (i % 10) < 5 ? 0 : 1;
        }
        Districting d = new Districting(g, 2, init, 0.02); // 2% of 500 = 10 = one unit of slack
        d.setWeights(new Weights(0, 0, 0, 0));
        assertTrue(d.allInside());
        SplittableRandom rng = Grids.rng(3);
        MoveKernel.Stats stats = new MoveKernel.Stats();
        for (int i = 0; i < 20_000; i++) {
            SingleFlip.INSTANCE.step(d, rng, 1e9, 0, stats);
            assertTrue(d.maxDeviation() <= 0.02 + 1e-12, "deviation " + d.maxDeviation());
        }
        assertTrue(stats.accepted > 100, "moves still happen inside the band: " + stats.accepted);
        // Moving one more unit from the smaller district would break the bound.
        int[] a = d.assignment();
        long pop0 = d.populationOf(0);
        int from = pop0 < 500 ? 0 : 1;
        if (d.populationOf(from) < 500) {
            for (int u = 0; u < 100; u++) {
                if (a[u] == from) {
                    assertTrue(Double.isNaN(d.evaluate(u, 1 - from)), "unit " + u);
                }
            }
        }
    }
    
    @Test
    void chainBringsEveryDistrictInsideTheTolerance() {
        Graph g = mixedGrid();
        ChainResult r = Chain.run(g, new Target(5, 2, 2.0, 0.025), Grids.schedule(40_000), Grids.BFS_VORONOI, SingleFlip.INSTANCE, 5, 0, Progress.Listener.NONE);
        assertTrue(r.populationValid(), "max deviation " + r.maxDeviation());
        assertTrue(r.maxDeviation() <= 0.025);
        assertEquals(3, r.phases().size());
    }
    
    @Test
    void pruningMoveTakesAOneTractWideTailAlong() {
        // District 0: the left half plus a tail along the top row to the right edge (like El Paso's
        // tail into Reeves County). Flipping the tail's first cell alone would cut the rest off.
        Graph g = Grids.lattice(10, 10, 10, i -> 0.5);
        int[] init = new int[100];
        for (int i = 0; i < 100; i++) {
            int r = i / 10;
            int c = i % 10;
            init[i] = c < 5 || r == 9 ? 0 : 1;
        }
        Districting d = new Districting(g, 2, init, 10);
        d.setWeights(new Weights(1, 0, 0, 0));
        int neck = 9 * 10 + 5; // top row, first tail cell
        assertTrue(!Double.isNaN(d.evaluate(neck, 1)));
        assertTrue(!d.removalKeepsConnected(neck), "the neck is a cut vertex");
        assertTrue(d.tryPieceMove(neck, 1, 1e9, new SplittableRandom(1)), "neck and tail move together");
        int[] a = d.assignment();
        for (int c = 5; c < 10; c++) {
            assertEquals(1, a[90 + c], "tail cell " + c);
        }
        d.verifyTallies();
        assertEquals(50, d.populationOf(0) / 10);
        // A piece move that makes population worse is rejected at a cold temperature and undone exactly:
        // district 0 has 46 cells, and moving its 6-cell tail away would leave 40 against 60.
        int[] small = new int[100];
        for (int i = 0; i < 100; i++) {
            small[i] = i % 10 < 4 || i / 10 == 9 ? 0 : 1;
        }
        Districting e = new Districting(g, 2, small, 10);
        e.setWeights(new Weights(1, 0, 0, 0));
        int neck2 = 9 * 10 + 4;
        e.evaluate(neck2, 1);
        assertTrue(!e.removalKeepsConnected(neck2));
        double before = e.score();
        assertTrue(!e.tryPieceMove(neck2, 1, 1e-30, new SplittableRandom(1)), "worse move rejected");
        assertEquals(java.util.Arrays.toString(small), java.util.Arrays.toString(e.assignment()));
        assertEquals(before, e.score(), 1e-9 * Math.max(1, Math.abs(before)));
        e.verifyTallies();
    }
}
