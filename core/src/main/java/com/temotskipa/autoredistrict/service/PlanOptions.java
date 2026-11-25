package com.temotskipa.autoredistrict.service;

import com.temotskipa.autoredistrict.engine.Annealer;

import java.time.Duration;

/**
 * Options for one plan request.
 *
 * @param demSeats      requested Democratic seats, or null for the proportional default
 * @param minMargin     two-party margin in points each targeted district should clear
 * @param seeds         chains run in parallel; the best is kept
 * @param baseSeed      seed of chain 0; chain i uses baseSeed + i
 * @param scheduleScale multiplies the step counts (1 = default)
 * @param timeBudget    per-chain wall-clock limit
 * @param tolerance     population tolerance as a fraction (0.01 = 1%)
 */
public record PlanOptions(Integer demSeats, double minMargin, int seeds, long baseSeed, double scheduleScale, Duration timeBudget, double tolerance) {
    
    public static final double DEFAULT_MARGIN = 2.0;
    public static final double DEFAULT_TOLERANCE = 0.01;
    public static final Duration DEFAULT_BUDGET = Duration.ofMinutes(12);
    
    public PlanOptions {
        if (seeds < 1 || seeds > Annealer.MAX_SEEDS) {
            throw new IllegalArgumentException("--seeds must be between 1 and " + Annealer.MAX_SEEDS);
        }
        // Written so NaN fails too: every comparison with NaN is false.
        if (!(Double.isFinite(minMargin) && minMargin >= 0)) {
            throw new IllegalArgumentException("--min-margin must be a number >= 0, not " + minMargin);
        }
        if (!(Double.isFinite(scheduleScale) && scheduleScale > 0)) {
            throw new IllegalArgumentException("--scale must be a number > 0, not " + scheduleScale);
        }
        if (!(tolerance > 0 && tolerance < 1)) {
            throw new IllegalArgumentException("the population tolerance must be between 0 and 1, not " + tolerance);
        }
    }
    
    public static PlanOptions defaults() {
        return new PlanOptions(null, DEFAULT_MARGIN, Annealer.defaultSeeds(), 1, 1.0, DEFAULT_BUDGET, DEFAULT_TOLERANCE);
    }
    
    /**
     * The proportional default: round(D two-party share x k), clamped to 0..k.
     */
    public static int proportional(double demShare, int k) {
        long n = Math.round(demShare * k);
        return Math.clamp(n, 0, k);
    }
    
    public PlanOptions withDemSeats(Integer n) {
        return new PlanOptions(n, minMargin, seeds, baseSeed, scheduleScale, timeBudget, tolerance);
    }
    
    public PlanOptions withSeeds(int s) {
        return new PlanOptions(demSeats, minMargin, s, baseSeed, scheduleScale, timeBudget, tolerance);
    }
    
    public PlanOptions withScale(double s) {
        return new PlanOptions(demSeats, minMargin, seeds, baseSeed, s, timeBudget, tolerance);
    }
    
    public PlanOptions withMargin(double m) {
        return new PlanOptions(demSeats, m, seeds, baseSeed, scheduleScale, timeBudget, tolerance);
    }
}
