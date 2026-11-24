package autoredistrict.engine;

import autoredistrict.engine.MoveKernel.Stats;
import autoredistrict.engine.Progress.Listener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CancellationException;

/**
 * Runs one Markov chain through a schedule: the video's three phases with exponential cooling in
 * each, then (in the last phase) more steps at the final temperature until every district is
 * inside the population tolerance. Deterministic for a given seed unless the time budget is hit.
 * Stops with a {@link CancellationException} when its thread is interrupted.
 */
public final class Chain {
    
    private static final int CALIBRATION_SAMPLES = 2000;
    
    private Chain() {
    }
    
    public static ChainResult run(Graph g, Target target, Schedule schedule, InitialMap initial, MoveKernel kernel, long seed, int chainIndex, Listener listener) {
        long start = System.nanoTime();
        long deadline = schedule.timeBudget() == null ? Long.MAX_VALUE : start + schedule.timeBudget().toNanos();
        SplittableRandom rng = new SplittableRandom(seed);
        int k = target.districts();
        int[] init = initial.assign(g, k, rng.split());
        Districting d = new Districting(g, k, init, target.populationTolerance());
        d.setTarget(target.demSeats(), target.minMarginPoints());
        var stats = new Stats();
        List<PhaseStats> phaseStats = new ArrayList<>();
        List<Phase> phases = schedule.phases();
        boolean timedOut = false;
        long reportedAt = start;
        long reportedProposals = 0;
        double temp = 1;
        for (int pi = 0; pi <= phases.size() && !timedOut; pi++) {
            boolean repair = pi == phases.size();
            if (repair && (d.allInside() || schedule.extraSteps() <= 0)) {
                break;
            }
            // After the last phase, if a district is still outside the tolerance: lift the hard
            // bound (so surplus can pass through full districts), weigh population 10x, and run
            // at the final temperature until every district is inside.
            Phase last = phases.getLast();
            Phase ph = repair
                    ? new Phase("repair", schedule.extraSteps(), new Weights(10 * last.weights().population(),
                            last.weights().compactness(), last.weights().tension(), last.weights().election()),
                            last.startAcceptance(), 1.0, last.selectionBias(), false)
                    : phases.get(pi);
            d.setWeights(ph.weights());
            d.setHardBound(ph.hardBound());
            d.recomputeTerms();
            double t0 = repair ? temp : calibrate(d, rng, ph, kernel);
            temp = t0;
            double factor = ph.steps() > 0 ? Math.pow(ph.coolTo(), 1.0 / ph.steps()) : 1.0;
            long accepted0 = stats.accepted;
            long phaseStart = System.nanoTime();
            long limit = ph.steps();
            long s = 0;
            for (; s < limit; s++) {
                kernel.step(d, rng, temp, ph.selectionBias(), stats);
                temp *= factor;
                if ((s & 0xFFF) == 0) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new CancellationException("chain " + chainIndex + " cancelled");
                    }
                    if (repair && d.allInside()) {
                        break;
                    }
                    if ((s & 0x3FFF) == 0) {
                        long now = System.nanoTime();
                        if (now > deadline) {
                            timedOut = true;
                            break;
                        }
                        if ((s & 0x3FFFF) == 0 && now > reportedAt) {
                            double fps = (stats.proposals - reportedProposals) / ((now - reportedAt) / 1e9);
                            reportedAt = now;
                            reportedProposals = stats.proposals;
                            listener.onProgress(new Progress(chainIndex, ph.name(), pi, phases.size(), s, ph.steps(), temp, d.score(), d.demWon(), d.maxDeviation(), fps));
                        }
                    }
                }
            }
            phaseStats.add(new PhaseStats(ph.name(), s, stats.accepted - accepted0, t0, temp, (System.nanoTime() - phaseStart) / 1e9));
        }
        d.verifyTallies();
        double secs = (System.nanoTime() - start) / 1e9;
        listener.onProgress(new Progress(chainIndex, "done", phases.size(), phases.size(), 0, 0, 0, d.score(), d.demWon(), d.maxDeviation(), secs > 0 ? stats.proposals / secs : 0));
        return new ChainResult(chainIndex, seed, d.assignment(), d.score(), d.populationTerm(), d.compactnessTerm(), d.tensionTerm(), d.electionPenalty(), d.demWon(), target.demSeats(), d.closestMargin(), d.maxDeviation(), d.allInside(), stats.proposals, stats.accepted, secs, List.copyOf(phaseStats), timedOut);
    }
    
    /**
     * Starting temperature: the median uphill change among sampled proposals, scored without the
     * population term (which is huge far from equality and would make the phase start far too
     * hot), divided by ln(1 / startAcceptance).
     */
    static double calibrate(Districting d, SplittableRandom rng, Phase ph, MoveKernel kernel) {
        Weights w = ph.weights();
        d.setWeights(new Weights(0, w.compactness(), w.tension(), w.election()));
        double[] ups = new double[CALIBRATION_SAMPLES];
        int m = 0;
        for (int i = 0; i < 2 * CALIBRATION_SAMPLES && m < ups.length; i++) {
            double delta = kernel.sampleDelta(d, rng, ph.selectionBias());
            if (delta > 0) {
                ups[m++] = delta;
            }
        }
        d.setWeights(w);
        if (m == 0) {
            return 1e-12;
        }
        Arrays.sort(ups, 0, m);
        return ups[m / 2] / Math.log(1 / ph.startAcceptance());
    }
}
