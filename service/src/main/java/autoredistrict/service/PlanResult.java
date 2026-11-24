package autoredistrict.service;

import autoredistrict.data.StateInfo;

import java.util.List;

/**
 * A finished plan request.
 *
 * @param flipsPerSecond proposed flips per second, summed over the chains
 * @param chainsHit      how many chains hit the target (any hit makes the request a hit)
 */
public record PlanResult(StateInfo state, int districts, int demRequested, boolean proportionalDefault, double minMargin, String voteData, double statewideDemShare, PlanMetrics metrics, int[] assignment, int seeds, long baseSeed, int bestChain, int chainsHit, long proposals, double flipsPerSecond, double seconds, String schedule, boolean atLarge, boolean timedOut, List<String> warnings) {
    
    public boolean hit() {
        return metrics.demWon() == demRequested;
    }
    
    /**
     * The PASS rules for this plan (the batch adds the 15-minute limit).
     */
    public boolean pass(double tolerance) {
        return metrics.pass(state.seats(), tolerance);
    }
}
