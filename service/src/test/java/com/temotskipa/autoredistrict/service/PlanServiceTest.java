package com.temotskipa.autoredistrict.service;

import com.temotskipa.autoredistrict.data.BlockVotes.Election;
import com.temotskipa.autoredistrict.data.StateDataset;
import com.temotskipa.autoredistrict.data.StateInfo;
import com.temotskipa.autoredistrict.data.States;
import com.temotskipa.autoredistrict.engine.Graph;
import com.temotskipa.autoredistrict.engine.Progress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.*;

class PlanServiceTest {
    
    @TempDir
    Path tmp;
    
    private PlanOptions fast(Integer dem) {
        return PlanOptions.defaults().withDemSeats(dem).withSeeds(2).withScale(2.0);
    }
    
    @Test
    void graphVoronoiGivesKContiguousNonEmptyDistricts() {
        StateInfo me = States.require("ME");
        SyntheticState.write(tmp, me, 20, 15, 1000);
        Graph g = StateDataset.load(tmp, me, Election.Y2024).graph();
        for (int k : new int[]{2, 5, 13}) {
            int[] a = GraphVoronoi.INSTANCE.assign(g, k, new SplittableRandom(k));
            boolean[] ok = PlanMetrics.contiguity(g, a, k);
            int[] sizes = new int[k];
            for (int d : a) {
                sizes[d]++;
            }
            for (int d = 0; d < k; d++) {
                assertTrue(ok[d], "district " + d + " of " + k + " contiguous");
                assertTrue(sizes[d] > 0, "district " + d + " non-empty");
            }
            assertArrayEquals(a, GraphVoronoi.INSTANCE.assign(g, k, new SplittableRandom(k)), "deterministic");
        }
    }
    
    @Test
    void atLargeStatesAreOneDistrictWithoutAnAnneal() throws InterruptedException {
        StateInfo vt = States.require("VT");
        SyntheticState.write(tmp, vt, 6, 6, 1000, 0.1);
        StateDataset ds = StateDataset.load(tmp, vt, Election.Y2024);
        PlanResult r = new PlanService(tmp).run(ds, PlanOptions.defaults(), p -> {
            throw new AssertionError("no chain should run");
        });
        assertTrue(r.atLarge());
        assertEquals(0, r.seeds());
        assertEquals(0, r.proposals());
        assertEquals(1, r.metrics().districts());
        assertTrue(java.util.Arrays.stream(r.assignment()).allMatch(d -> d == 0));
        assertEquals(36_000, r.metrics().population());
        assertTrue(r.pass());
        assertEquals(1, r.demRequested(), "D share 0.6 rounds to the single seat");
        assertTrue(r.hit());
    }
    
    @Test
    void nonFiniteOptionsAreRefused() {
        PlanOptions o = PlanOptions.defaults();
        for (double bad : new double[]{Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThrows(IllegalArgumentException.class, () -> o.withScale(bad), "scale " + bad);
            assertThrows(IllegalArgumentException.class, () -> o.withMargin(bad), "margin " + bad);
            assertThrows(IllegalArgumentException.class, () -> new PlanOptions(null, 2.0, 2, 1, 1.0, PlanOptions.DEFAULT_BUDGET, bad), "tolerance " + bad);
        }
        assertThrows(IllegalArgumentException.class, () -> o.withScale(-1));
        assertEquals(3.0, o.withScale(3.0).scheduleScale());
    }

    @Test
    void proportionalDefaultIsRoundedAndClamped() {
        assertEquals(7, PlanOptions.proportional(0.4837, 14)); // NC 2024: 6.77
        assertEquals(16, PlanOptions.proportional(0.4306, 38)); // TX 2024: 16.36
        assertEquals(1, PlanOptions.proportional(0.5354, 2)); // ME
        assertEquals(0, PlanOptions.proportional(-0.2, 5));
        assertEquals(5, PlanOptions.proportional(1.3, 5));
        assertEquals(0, PlanOptions.proportional(0.45, 1));
        assertEquals(1, PlanOptions.proportional(0.5, 1));
    }
    
    @Test
    void runsATwoDistrictStateToAPassingPlanAndWritesArtifacts() throws Exception {
        StateInfo me = States.require("ME");
        SyntheticState.write(tmp, me, 12, 10, 1000);
        PlanService svc = new PlanService(tmp);
        StateDataset ds = svc.load(me, Election.Y2024);
        PlanResult r = svc.run(ds, fast(1), Progress.Listener.NONE);
        assertTrue(r.pass(), "max dev " + r.metrics().maxDeviation());
        assertTrue(r.hit());
        assertEquals(2, r.metrics().rows().size());
        List<Path> files = Artifacts.write(r, ds, tmp.resolve("out"), "me", true);
        for (Path f : files) {
            assertTrue(Files.size(f) > 0, f.toString());
        }
        assertTrue(Files.readString(tmp.resolve("out/me.geojson")).contains("\"FeatureCollection\""));
    }

    @Test
    void reportBatchAndSeatRangeJudgePassWithTheRequestTolerance() throws Exception {
        StateInfo me = States.require("ME");
        // 225 equal tracts can't split evenly in two: the best plan is about 0.44% off, inside 1%.
        SyntheticState.write(tmp, me, 15, 15, 1000);
        PlanService svc = new PlanService(tmp);
        StateDataset ds = svc.load(me, Election.Y2024);
        PlanResult r = svc.run(ds, fast(1), Progress.Listener.NONE);
        assertTrue(r.pass());
        assertTrue(r.metrics().maxDeviation() > 1e-6);
        // The same plan from a request with a 0.0001% tolerance: every view must call it a FAIL.
        PlanResult strict = new PlanResult(r.state(), r.districts(), r.demRequested(), r.proportionalDefault(), r.minMargin(), 1e-6, r.voteData(), r.statewideDemShare(), r.metrics(), r.assignment(), r.seeds(), r.baseSeed(), r.bestChain(), r.chainsHit(), r.proposals(), r.flipsPerSecond(), r.seconds(), r.schedule(), r.atLarge(), r.timedOut(), r.warnings());
        assertFalse(strict.pass());
        Path report = tmp.resolve("strict.json");
        Artifacts.writeReport(strict, ds, report);
        var json = com.temotskipa.autoredistrict.data.Json.mapper().readTree(Files.readString(report));
        assertFalse(json.get("pass").asBoolean());
        assertEquals(1e-4, json.get("tolerancePct").asDouble(), 1e-12, "the report states the tolerance it used");
        assertFalse(BatchRunner.row(strict).pass());
        assertFalse(SeatRange.rowOf(strict).pass());
    }

    @Test
    void seatRangeStepsOutwardAndStopsEachDirectionAtTheFirstMiss() throws InterruptedException {
        List<Integer> asked = new ArrayList<>();
        List<SeatRange.Row> rows = SeatRange.run(10, 5, n -> {
            asked.add(n);
            boolean hit = n >= 3 && n <= 7;
            return new SeatRange.Row(n, hit ? n : n - 1, 0, hit, 2.0, 0.005, true, true, "2024", 4, 1.0, 1e6);
        });
        assertEquals(List.of(5, 6, 7, 8, 4, 3, 2), asked);
        assertEquals(java.util.Set.of(3, 4, 5, 6, 7), SeatRange.distinctHits(rows));
        // A hit that fails PASS doesn't count and stops the direction.
        List<Integer> asked2 = new ArrayList<>();
        SeatRange.run(4, 2, n -> {
            asked2.add(n);
            boolean pass = n != 3;
            return new SeatRange.Row(n, n, 4 - n, true, 2.0, pass ? 0.005 : 0.02, true, pass, "2024", 4, 1.0, 1e6);
        });
        assertEquals(List.of(2, 3, 1, 0), asked2);
        // The proportional request is always made, and the edges stop at 0 and k.
        List<Integer> asked3 = new ArrayList<>();
        SeatRange.run(2, 2, n -> {
            asked3.add(n);
            return new SeatRange.Row(n, n, 2 - n, true, 2.0, 0.005, true, true, "2024", 4, 1.0, 1e6);
        });
        assertEquals(List.of(2, 1, 0), asked3);
        String table = SeatRange.table("NC", 14, rows);
        assertTrue(table.contains("5 distinct D seat counts hit: [3, 4, 5, 6, 7]"), table);
    }
    
    @Test
    void batchRunsEveryStateAndSummarisesPassesHitsAndTime() throws Exception {
        StateInfo me = States.require("ME");
        StateInfo vt = States.require("VT");
        SyntheticState.write(tmp, me, 12, 10, 1000);
        SyntheticState.write(tmp, vt, 5, 5, 1000);
        Path out = tmp.resolve("all");
        List<String> log = new ArrayList<>();
        BatchRunner.Summary s = BatchRunner.run(new PlanService(tmp), List.of(me, vt, States.require("NH")), fast(null), Election.Y2024, out, log::add);
        assertEquals(3, s.rows().size());
        assertEquals("PASS", s.rows().get(0).status());
        assertEquals("PASS", s.rows().get(1).status());
        assertEquals("ERROR", s.rows().get(2).status(), "NH isn't in the cache");
        assertEquals(2, s.passCount());
        assertEquals(3, s.totalSeats());
        String md = BatchRunner.markdown(s);
        assertTrue(md.contains("3 states, 3 seats in total; 2 PASS"), md);
        assertTrue(md.contains("slowest state"), md);
        assertTrue(Files.size(out.resolve("contact-sheet.png")) > 0);
        assertTrue(Files.readString(out.resolve("summary.csv")).startsWith("state,fips,seats"));
        assertTrue(Files.exists(out.resolve("states/me.png")));
        BatchRunner.Row slow = new BatchRunner.Row("XX", "99", 2, 10, 10, 0.1, true, true, true, 1, 1, 1, true, 3, 1, BatchRunner.STATE_LIMIT_SECONDS + 1, "2024", "PASS", null);
        assertFalse(new BatchRunner.Summary(List.of(slow), 1, 2).slowest() == null);
    }
}
