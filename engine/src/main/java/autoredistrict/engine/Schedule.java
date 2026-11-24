package autoredistrict.engine;

import java.time.Duration;
import java.util.List;

/**
 * The phases of an anneal. After the last phase the chain keeps going at its final temperature
 * for up to {@code extraSteps} more proposals until every district is within the population
 * tolerance. {@code timeBudget} (may be null) stops a chain early; results then say so.
 */
public record Schedule(List<Phase> phases, long extraSteps, Duration timeBudget) {
    
    public Schedule {
        phases = List.copyOf(phases);
        if (phases.isEmpty()) {
            throw new IllegalArgumentException("a schedule needs at least one phase");
        }
    }
    
    public long totalSteps() {
        return phases.stream().mapToLong(Phase::steps).sum();
    }
}
