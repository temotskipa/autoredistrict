package autoredistrict.engine;

import java.util.SplittableRandom;

/**
 * The video's move: pick a boundary unit (biased toward units with more foreign neighbours),
 * flip it to a neighbouring district, keep it if better or with probability exp(-delta/T) if
 * worse, and only if the district it leaves stays contiguous. Contiguity is checked after the
 * Metropolis test, which gives the same chain but skips the check for rejected moves.
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
        double delta = d.evaluate(u, b);
        if (Double.isNaN(delta)) {
            return false;
        }
        if (delta > 0 && rng.nextDouble() >= Math.exp(-delta / t)) {
            return false;
        }
        if (!d.removalKeepsConnected(u)) {
            // u is a cut vertex. If what it cuts off is small, try taking it along.
            if (d.tryPieceMove(u, b, t, rng)) {
                stats.accepted++;
                stats.pieceMoves++;
                return true;
            }
            return false;
        }
        d.commit();
        stats.accepted++;
        return true;
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
