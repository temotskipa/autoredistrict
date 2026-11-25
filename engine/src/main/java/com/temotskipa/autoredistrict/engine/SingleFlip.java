package com.temotskipa.autoredistrict.engine;

import java.util.SplittableRandom;

/**
 * The video's move: pick a boundary unit (biased toward units with more foreign neighbours),
 * flip it to a neighbouring district, keep it if better or with probability exp(-delta/T) if
 * worse, and only if the district it leaves stays contiguous. When the unit is a cut vertex, the
 * move is the pruning move instead (the unit and the small piece it cuts off), which takes one
 * Metropolis test of its own on the whole move's score change.
 */
public final class SingleFlip implements MoveKernel {

    public static final SingleFlip INSTANCE = new SingleFlip();

    private SingleFlip() {
    }

    @Override
    public boolean step(Districting d, SplittableRandom rng, double t, double selectionBias, Stats stats) {
        int u = d.pickBoundary(rng, selectionBias);
        if (u < 0) {
            return false;
        }
        int b = d.pickTarget(u, rng);
        stats.proposals++;
        return tryFlip(d, u, b, t, rng, stats);
    }

    /**
     * Proposes moving {@code u} to district {@code b} and applies it if accepted.
     */
    boolean tryFlip(Districting d, int u, int b, double t, SplittableRandom rng, Stats stats) {
        double delta = d.evaluate(u, b);
        if (Double.isNaN(delta)) {
            return false;
        }
        // The Metropolis test comes first so that most rejected flips skip the full contiguity
        // check. Whatever it said, a cut vertex's flip alone is never made: if it cuts off a small
        // piece, the pruning move is tried instead, on its own test of the summed delta (the draw
        // above is independent of that test, so it doesn't change the move's probability).
        boolean pass = delta <= 0 || rng.nextDouble() < Math.exp(-delta / t);
        if (pass && d.removalKeepsConnected(u)) {
            d.commit();
            stats.accepted++;
            return true;
        }
        if (d.cutsOffSmallPiece(u) && d.tryPieceMove(u, b, t, rng)) {
            stats.accepted++;
            stats.pieceMoves++;
            return true;
        }
        return false;
    }

    @Override
    public double sampleDelta(Districting d, SplittableRandom rng, double selectionBias) {
        int u = d.pickBoundary(rng, selectionBias);
        if (u < 0) {
            return Double.NaN;
        }
        return d.evaluate(u, d.pickTarget(u, rng));
    }
}
