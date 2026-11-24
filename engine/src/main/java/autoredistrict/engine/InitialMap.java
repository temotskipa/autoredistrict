package autoredistrict.engine;

import java.util.SplittableRandom;

/**
 * Builds a contiguous starting assignment of the graph's units to {@code k} districts.
 */
@FunctionalInterface
public interface InitialMap {
    
    int[] assign(Graph graph, int k, SplittableRandom rng);
}
