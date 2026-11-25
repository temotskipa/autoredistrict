package com.temotskipa.autoredistrict.service;

import com.temotskipa.autoredistrict.data.BlockVotes.Election;
import com.temotskipa.autoredistrict.data.DataException;
import com.temotskipa.autoredistrict.data.StateDataset;
import com.temotskipa.autoredistrict.data.StateInfo;
import com.temotskipa.autoredistrict.engine.*;
import com.temotskipa.autoredistrict.engine.Progress.Listener;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs plan requests on cached states. Both front ends call this; it never imports or touches
 * the network.
 */
public record PlanService(Path cacheRoot) {
    
    public StateDataset load(StateInfo state, Election election) {
        return StateDataset.load(cacheRoot, state, election);
    }
    
    /**
     * Runs every chain to the end and keeps the best.
     */
    public PlanResult run(StateDataset ds, PlanOptions o, Listener listener) throws InterruptedException {
        return run(ds, o, listener, false);
    }
    
    /**
     * Like {@link #run} but stops at the first chain in seed order that hits the target within the
     * population tolerance, once the chains before it have finished, and cancels the rest (used by
     * seat-range). The plan is that chain's, so it is the same on every run with the same seed.
     */
    public PlanResult runUntilHit(StateDataset ds, PlanOptions o, Listener listener) throws InterruptedException {
        return run(ds, o, listener, true);
    }
    
    private PlanResult run(StateDataset ds, PlanOptions o, Listener listener, boolean untilHit) throws InterruptedException {
        long t0 = System.nanoTime();
        StateInfo st = ds.state();
        int k = st.seats();
        double share = ds.demShare();
        int proportional = PlanOptions.proportional(share, k);
        int dem = o.demSeats() == null ? proportional : o.demSeats();
        if (dem < 0 || dem > k) {
            throw new DataException("--dem-seats for " + st.abbr() + " must be between 0 and " + k + ", not " + dem + ".");
        }
        Graph g = ds.graph();
        List<String> warnings = new ArrayList<>(ds.warnings());
        int[] assignment;
        int seeds;
        int bestChain = -1;
        int chainsHit = 0;
        long proposals = 0;
        boolean timedOut = false;
        String schedule;
        if (k == 1) {
            // At-large: the whole state is the district. No anneal.
            assignment = new int[g.size()];
            seeds = 0;
            schedule = "at-large (no anneal)";
        }
        else {
            Schedule s = Schedules.forTracts(g.size(), o.scheduleScale(), o.timeBudget());
            schedule = Schedules.describe(s);
            var target = new Target(k, dem, o.minMargin(), o.tolerance());
            long[] seedList = Annealer.seeds(o.baseSeed(), o.seeds());
            List<ChainResult> rs;
            if (untilHit) {
                // Cancelled chains count toward flips per second too.
                Annealer.Until until = Annealer.runUntil(g, target, s, GraphVoronoi.INSTANCE, seedList, listener, r -> r.hit() && r.populationValid());
                rs = until.finished();
                proposals = until.proposals();
            }
            else {
                rs = Annealer.runAll(g, target, s, GraphVoronoi.INSTANCE, seedList, listener);
                for (ChainResult r : rs) {
                    proposals += r.proposals();
                }
            }
            ChainResult best = Annealer.best(rs);
            assignment = best.assignment();
            seeds = o.seeds();
            bestChain = best.chain();
            for (ChainResult r : rs) {
                if (r.hit()) {
                    chainsHit++;
                }
                timedOut |= r.timedOut();
            }
            if (timedOut) {
                warnings.add("A chain stopped at its time budget of " + o.timeBudget().toMinutes() + " min.");
            }
        }
        if (k > 1) {
            EmptyTractCleanup.apply(g, assignment, k);
        }
        PlanMetrics m = PlanMetrics.compute(ds, assignment, k);
        double secs = (System.nanoTime() - t0) / 1e9;
        return new PlanResult(st, k, dem, o.demSeats() == null, o.minMargin(), o.tolerance(), ds.voteDataLabel(), share, m, assignment, seeds, o.baseSeed(), bestChain, chainsHit, proposals, secs > 0 ? proposals / secs : 0, secs, schedule, k == 1, timedOut, List.copyOf(warnings));
    }
}
