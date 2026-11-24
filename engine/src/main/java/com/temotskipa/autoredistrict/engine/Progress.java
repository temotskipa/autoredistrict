package com.temotskipa.autoredistrict.engine;

/**
 * A progress report from one chain. A value object.
 */
public value record Progress(int chain, String phase, int phaseIndex, int phaseCount, long step, long phaseSteps, double temperature, double score, int demWon, double maxDeviation, double flipsPerSecond) {
    
    /**
     * Receives progress from chain threads; implementations must be thread safe.
     */
    @FunctionalInterface
    public interface Listener {
        
        Listener NONE = _ -> {
        };
        
        void onProgress(Progress progress);
    }
}
