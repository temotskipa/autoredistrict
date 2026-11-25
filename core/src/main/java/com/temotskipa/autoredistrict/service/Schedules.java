package com.temotskipa.autoredistrict.service;

import com.temotskipa.autoredistrict.engine.Phase;
import com.temotskipa.autoredistrict.engine.Schedule;
import com.temotskipa.autoredistrict.engine.Weights;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * The three-phase schedule from the video, with step counts proportional to the tract count so
 * every state gets the same number of proposals per tract:
 * <ol>
 *   <li><b>shape</b>: population and centring weigh most; equal, roughly round districts;</li>
 *   <li><b>explore</b>: lower surface tension and a hotter start, so districts grow fingers and
 *       "sense" the voters around them, with a little election pressure;</li>
 *   <li><b>elect</b>: full election-fit weight and surface tension back on, cooling to frozen,
 *       then extra steps until every district is within the population tolerance.</li>
 * </ol>
 */
public final class Schedules {
    
    /**
     * Proposals per tract in each phase at scale 1.
     */
    public static final long SHAPE_PER_TRACT = 1200;
    public static final long EXPLORE_PER_TRACT = 1200;
    public static final long ELECT_PER_TRACT = 3600;
    public static final long EXTRA_PER_TRACT = 2400;
    
    public static final Weights SHAPE = new Weights(1, 50, 2, 0);
    public static final Weights EXPLORE = new Weights(1, 20, 0.5, 0.3);
    public static final Weights ELECT = new Weights(1, 20, 2, 1);
    
    private Schedules() {
    }
    
    public static Schedule forTracts(int tracts, double scale, Duration budget) {
        double u = tracts * scale;
        return new Schedule(List.of(new Phase("shape", Math.round(SHAPE_PER_TRACT * u), SHAPE, 0.5, 1e-3, 1.0, false), new Phase("explore", Math.round(EXPLORE_PER_TRACT * u), EXPLORE, 0.8, 1e-2, 0.25, false), new Phase("elect", Math.round(ELECT_PER_TRACT * u), ELECT, 0.5, 1e-4, 1.0, true)), Math.round(EXTRA_PER_TRACT * u), budget);
    }
    
    /**
     * "shape 2.7M, explore 2.7M, elect 8.3M (+5.5M)" for reports.
     */
    public static String describe(Schedule s) {
        String phases = s.phases().stream().map(p -> p.name() + " " + millions(p.steps())).collect(Collectors.joining(", "));
        return phases + " (+" + millions(s.extraSteps()) + " until in tolerance)";
    }
    
    private static String millions(long n) {
        return String.format(Locale.ROOT, "%.1fM", n / 1e6);
    }
}
