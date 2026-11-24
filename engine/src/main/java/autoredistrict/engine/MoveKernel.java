package autoredistrict.engine;

import java.util.SplittableRandom;

/**
 * One Metropolis proposal on a districting. The single-unit flip is the only kernel today; a
 * second one (for example ReCom's merge-split, used by the "short bursts" method) can be added
 * behind this interface without touching the chain loop.
 */
public interface MoveKernel {
    
    /**
     * Proposes and maybe applies one move at temperature {@code t}. Returns true if accepted.
     */
    boolean step(Districting d, SplittableRandom rng, double t, double selectionBias, Stats stats);
    
    /**
     * Change in score of a random proposal, without applying it; NaN if forbidden or none.
     */
    double sampleDelta(Districting d, SplittableRandom rng, double selectionBias);
    
    /**
     * Outcome counters a kernel updates.
     */
    final class Stats {
        public long proposals;
        public long accepted;
    }
}
