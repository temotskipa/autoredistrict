package com.temotskipa.autoredistrict.engine;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class AnnealerTest {
    
    /**
     * A 24x24 grid with planted voters: D share falls from 75% on the left to 25% on the right,
     * so the statewide vote is even and both packed and cracked maps exist.
     */
    private static Graph planted() {
        return Grids.lattice(24, 24, 100, i -> 0.75 - 0.5 * (i % 24) / 23.0);
    }
    
    private static long chainThreads() {
        return Thread.getAllStackTraces().keySet().stream().filter(t -> t.getName().startsWith("chain-") && t.isAlive()).count();
    }
    
    @Test
    void hitsSeveralRequestedSeatCountsOnAPlantedGrid() throws InterruptedException {
        Graph g = planted();
        Set<Integer> hit = new TreeSet<>();
        for (int dem = 1; dem <= 5; dem++) {
            List<ChainResult> rs = Annealer.runAll(g, new Target(6, dem, 2.0, 0.025), Grids.schedule(60_000), Grids.BFS_VORONOI, Annealer.seeds(100, 4), Progress.Listener.NONE);
            ChainResult best = Annealer.best(rs);
            assertTrue(best.populationValid(), "dem " + dem + ": deviation " + best.maxDeviation());
            if (best.hit()) {
                hit.add(dem);
            }
        }
        assertTrue(hit.containsAll(List.of(2, 3, 4)), "hit " + hit);
        assertTrue(hit.size() >= 4, "hit " + hit);
    }
    
    @Test
    void parallelSeedsGiveTheSameResultAsRunningThemOneByOne() throws InterruptedException {
        Graph g = planted();
        Target t = new Target(4, 2, 2.0, 0.02);
        Schedule s = Grids.schedule(15_000);
        long[] seeds = Annealer.seeds(42, 4);
        List<ChainResult> parallel = Annealer.runAll(g, t, s, Grids.BFS_VORONOI, seeds, Progress.Listener.NONE);
        for (int i = 0; i < seeds.length; i++) {
            ChainResult one = Chain.run(g, t, s, Grids.BFS_VORONOI, SingleFlip.INSTANCE, seeds[i], i, Progress.Listener.NONE);
            assertArrayEquals(one.assignment(), parallel.get(i).assignment(), "seed " + seeds[i]);
            assertEquals(one.score(), parallel.get(i).score(), 0.0);
            assertEquals(one.proposals(), parallel.get(i).proposals());
        }
        assertEquals(Annealer.best(parallel).chain(), Annealer.best(new ArrayList<>(parallel).reversed()).chain());
    }
    
    @Test
    void interruptingTheCallerCancelsTheScopeAndStopsEveryChain() throws Exception {
        Graph g = planted();
        Schedule endless = new Schedule(List.of(new Phase("long", Long.MAX_VALUE / 4, new Weights(1, 10, 1, 1), 0.5, 1e-3, 1.0, false)), 0, Duration.ofMinutes(10));
        AtomicReference<Throwable> outcome = new AtomicReference<>();
        Thread caller = Thread.ofPlatform().start(() -> {
            try {
                Annealer.runAll(g, new Target(4, 2, 2.0, 0.02), endless, Grids.BFS_VORONOI, Annealer.seeds(1, 4), Progress.Listener.NONE);
                outcome.set(new AssertionError("runAll returned"));
            } catch (Throwable e) {
                outcome.set(e);
            }
        });
        Thread.sleep(400);
        assertTrue(chainThreads() > 0, "chains should be running");
        long t0 = System.nanoTime();
        caller.interrupt();
        caller.join(10_000);
        assertFalse(caller.isAlive(), "caller should return promptly");
        assertTrue((System.nanoTime() - t0) / 1e9 < 5, "cancellation took too long");
        assertInstanceOf(InterruptedException.class, outcome.get(), "got " + outcome.get());
        assertEquals(0, chainThreads(), "every chain thread stopped");
    }
    
    @Test
    void runUntilStopsTheOtherChainsAfterAMatch() throws InterruptedException {
        Graph g = planted();
        Annealer.Until u = Annealer.runUntil(g, new Target(4, 2, 2.0, 0.02), Grids.schedule(10_000), Grids.BFS_VORONOI, Annealer.seeds(9, 3), Progress.Listener.NONE, r -> true);
        List<ChainResult> rs = u.finished();
        assertTrue(!rs.isEmpty() && rs.size() <= 3);
        long finished = rs.stream().mapToLong(ChainResult::proposals).sum();
        assertTrue(u.proposals() >= finished, "every chain's proposals are counted: " + u.proposals() + " < " + finished);
        assertEquals(0, chainThreads());
    }
}
