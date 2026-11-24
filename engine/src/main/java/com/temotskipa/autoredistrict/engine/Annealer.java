package com.temotskipa.autoredistrict.engine;

import java.util.ArrayList;
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
        long[] s = new long[count];
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
        Semaphore cores = new Semaphore(Math.min(seeds.length, Runtime.getRuntime().availableProcessors()));
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
     * Runs the chains until one finishes with a result {@code stop} accepts, then cancels the
     * rest (structured concurrency's {@code Joiner.allUntil}).
     */
    public static Until runUntil(Graph g, Target target, Schedule schedule, InitialMap initial, long[] seeds, Progress.Listener listener, Predicate<ChainResult> stop) throws InterruptedException {
        checkSeedCount(seeds);
        Semaphore cores = new Semaphore(Math.min(seeds.length, Runtime.getRuntime().availableProcessors()));
        // Subtasks that end after the scope is cancelled report nothing, so every chain adds its own
        // proposals here, finished or cancelled. The scope's close waits for all of them.
        LongAdder proposals = new LongAdder();
        List<Subtask<ChainResult>> done;
        try (var scope = StructuredTaskScope.open(Joiner.<ChainResult>allUntil(t -> t.state() == State.SUCCESS && stop.test(t.get())), cfg -> cfg.withName("anneal-until").withThreadFactory(Thread.ofPlatform().name("chain-", 0).factory()))) {
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
            done = scope.join();
        }
        List<ChainResult> results = new ArrayList<>();
        for (Subtask<ChainResult> t : done) {
            switch (t.state()) {
                case SUCCESS -> results.add(t.get());
                case FAILED -> {
                    Throwable ex = t.exception();
                    if (!(ex instanceof CancellationException) && !(ex instanceof InterruptedException)) {
                        throw rethrow(ex);
                    }
                }
                case UNAVAILABLE -> {
                }
            }
        }
        results.sort(Comparator.comparingInt(ChainResult::chain));
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
        if (t instanceof RuntimeException r) {
            return r;
        }
        if (t instanceof Error e) {
            throw e;
        }
        return new IllegalStateException(t);
    }
    
    /**
     * What {@link #runUntil} returns.
     *
     * @param finished  the chains whose results are available, in seed order
     * @param proposals proposals made by every chain, including the cancelled ones
     */
    public record Until(List<ChainResult> finished, long proposals) {
    }
}
