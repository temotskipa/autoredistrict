package com.temotskipa.autoredistrict.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Joiner;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.concurrent.StructuredTaskScope.Subtask.State;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

/**
 * Runs several chains for one request in parallel, as subtasks of one {@link StructuredTaskScope}
 * on platform threads (the work is CPU bound, so virtual threads would add nothing), at most one
 * per core. Interrupting the calling thread cancels the scope, which interrupts every chain; the
 * GUI's Cancel button relies on that.
 */
public final class Annealer {
    
    /**
     * Most chains one request may run. Each chain is a platform thread from the start (it then waits
     * for a free core), so the count is capped rather than left to exhaust native threads.
     */
    public static final int MAX_SEEDS = 256;
    
    private Annealer() {
    }
    
    /**
     * Default chains per request: the number of cores, capped at 16.
     */
    public static int defaultSeeds() {
        return Math.clamp(Runtime.getRuntime().availableProcessors(), 1, 16);
    }
    
    /**
     * Seeds {@code base, base+1, ...}.
     */
    public static long[] seeds(long base, int count) {
        var s = new long[count];
        for (int i = 0; i < count; i++) {
            s[i] = base + i;
        }
        return s;
    }
    
    /**
     * Runs every chain to the end; results are in seed order.
     */
    public static List<ChainResult> runAll(Graph g, Target target, Schedule schedule, InitialMap initial, long[] seeds, Progress.Listener listener) throws InterruptedException {
        checkSeedCount(seeds);
        var cores = new Semaphore(Math.min(seeds.length, Runtime.getRuntime().availableProcessors()));
        try (var scope = StructuredTaskScope.open(Joiner.<ChainResult>allSuccessfulOrThrow(), cfg -> cfg.withName("anneal").withThreadFactory(Thread.ofPlatform().name("chain-", 0).factory()))) {
            for (int i = 0; i < seeds.length; i++) {
                int index = i;
                scope.fork(() -> runLimited(cores, g, target, schedule, initial, seeds[index], index, listener));
            }
            List<ChainResult> results = new ArrayList<>(scope.join());
            results.sort(Comparator.comparingInt(ChainResult::chain));
            return results;
        } catch (ExecutionException e) {
            throw rethrow(e.getCause());
        }
    }
    
    /**
     * Runs the chains until the first one in seed order whose result {@code stop} accepts has
     * finished, along with every chain before it, then cancels the rest. Waiting for the earlier
     * chains makes the outcome independent of which chain happens to finish first, so a seed gives
     * the same plan on every run.
     */
    public static Until runUntil(Graph g, Target target, Schedule schedule, InitialMap initial, long[] seeds, Progress.Listener listener, Predicate<ChainResult> stop) throws InterruptedException {
        checkSeedCount(seeds);
        // Fair, so that with more chains than cores the earlier chains, which decide, start first.
        var cores = new Semaphore(Math.min(seeds.length, Runtime.getRuntime().availableProcessors()), true);
        // Subtasks that end after the scope is cancelled report nothing, so every chain adds its own
        // proposals here, finished or cancelled. The scope's close waits for all of them.
        var proposals = new LongAdder();
        List<ChainResult> results;
        try (var scope = StructuredTaskScope.open(new FirstInSeedOrder(stop, seeds.length), cfg -> cfg.withName("anneal-until").withThreadFactory(Thread.ofPlatform().name("chain-", 0).factory()))) {
            for (int i = 0; i < seeds.length; i++) {
                int index = i;
                scope.fork(() -> {
                    try {
                        ChainResult r = runLimited(cores, g, target, schedule, initial, seeds[index], index, listener);
                        proposals.add(r.proposals());
                        return r;
                    } catch (Chain.Cancelled c) {
                        proposals.add(c.proposals());
                        throw c;
                    }
                });
            }
            results = scope.join();
        }
        return new Until(results, proposals.sum());
    }
    
    private static void checkSeedCount(long[] seeds) {
        if (seeds.length < 1 || seeds.length > MAX_SEEDS) {
            throw new IllegalArgumentException("between 1 and " + MAX_SEEDS + " chains per request, not " + seeds.length);
        }
    }
    
    private static ChainResult runLimited(Semaphore cores, Graph g, Target target, Schedule schedule, InitialMap initial, long seed, int index, Progress.Listener listener) throws InterruptedException {
        cores.acquire();
        try {
            return Chain.run(g, target, schedule, initial, SingleFlip.INSTANCE, seed, index, listener);
        } finally {
            cores.release();
        }
    }
    
    /**
     * The best result: population valid, then hit, then lowest score.
     */
    public static ChainResult best(List<ChainResult> results) {
        return results.stream().min(ChainResult.BEST).orElseThrow(() -> new IllegalStateException("no chain finished"));
    }
    
    private static RuntimeException rethrow(Throwable t) {
        return switch (t) {
            case RuntimeException r -> r;
            case Error e -> throw e;
            default -> new IllegalStateException(t);
        };
    }
    
    /**
     * What {@link #runUntil} returns.
     *
     * @param finished  chains 0 to the first accepted one (all of them if none was), in seed order
     * @param proposals proposals made by every chain, including the cancelled ones
     */
    public value record Until(List<ChainResult> finished, long proposals) {
    }
    
    /**
     * {@link #runUntil}'s joiner: cancels the scope once chains 0..i have all finished and i is
     * the first that {@code stop} accepts, and returns those chains. Chains that finish out of
     * order are held until the ones before them have finished. A chain failing with anything but
     * cancellation cancels the scope, and {@link StructuredTaskScope#join} throws its exception.
     */
    private static final class FirstInSeedOrder implements Joiner<ChainResult, List<ChainResult>, RuntimeException> {
        private final Predicate<ChainResult> stop;
        private final ChainResult[] results;
        private int next; // chains 0..next-1 finished and none was accepted
        private int accepted = -1;
        private Throwable failure;
        
        FirstInSeedOrder(Predicate<ChainResult> stop, int chains) {
            this.stop = stop;
            this.results = new ChainResult[chains];
        }
        
        @Override
        public synchronized boolean onComplete(Subtask<ChainResult> t) {
            if (accepted >= 0 || failure != null) {
                return true;
            }
            if (t.state() == State.FAILED) {
                return switch (t.exception()) {
                    case CancellationException _, InterruptedException _ -> false;
                    case Throwable ex -> {
                        failure = ex;
                        yield true;
                    }
                };
            }
            if (t.state() != State.SUCCESS) {
                return false;
            }
            ChainResult r = t.get();
            results[r.chain()] = r;
            while (next < results.length && results[next] != null) {
                if (stop.test(results[next])) {
                    accepted = next;
                    return true;
                }
                next++;
            }
            return false;
        }
        
        @Override
        public synchronized List<ChainResult> result() {
            if (failure != null) {
                throw rethrow(failure);
            }
            return List.of(Arrays.copyOf(results, accepted >= 0 ? accepted + 1 : next));
        }
        
        @Override
        public List<ChainResult> timeout() {
            return result(); // the scope has no timeout; chains keep their own time budget
        }
    }
}
