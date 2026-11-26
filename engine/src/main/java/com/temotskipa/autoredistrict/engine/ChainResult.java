package com.temotskipa.autoredistrict.engine;

import java.util.Comparator;
import java.util.List;

/**
 * The final state of one chain.
 *
 * @param populationValid every district within the population tolerance
 * @param proposals       proposed flips evaluated (the "flips" in flips per second)
 */
public value record ChainResult(int chain, long seed, int[] assignment, double score, double populationTerm, double compactnessTerm, double tensionTerm, double electionPenalty, int demWon, int demTarget, double closestMargin, double maxDeviation, boolean populationValid, long proposals, long accepted, double seconds, List<PhaseStats> phases, boolean timedOut) {
    
    /**
     * Best first: population valid, then hitting the seat target, then lowest score.
     */
    public static final Comparator<ChainResult> BEST = Comparator.comparing((ChainResult r) -> !r.populationValid()).thenComparing(r -> !r.hit()).thenComparingDouble(ChainResult::score).thenComparingInt(ChainResult::chain);
    
    public boolean hit() {
        return demWon == demTarget;
    }
}
