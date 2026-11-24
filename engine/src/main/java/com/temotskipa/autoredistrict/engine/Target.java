package com.temotskipa.autoredistrict.engine;

/**
 * What a run asks for: {@code districts} seats, of which {@code demSeats} should be won by
 * Democrats, each targeted district clearing {@code minMarginPoints} of two-party margin, with
 * every district within {@code populationTolerance} (a fraction, 0.01 = 1%) of ideal.
 * A value object.
 */
public value record Target(int districts, int demSeats, double minMarginPoints, double populationTolerance) {
    
    public Target {
        if (districts < 1) {
            throw new IllegalArgumentException("districts must be at least 1");
        }
        if (demSeats < 0 || demSeats > districts) {
            throw new IllegalArgumentException("dem seats " + demSeats + " outside 0.." + districts);
        }
        if (minMarginPoints < 0 || populationTolerance <= 0) {
            throw new IllegalArgumentException("margin must be >= 0 and tolerance > 0");
        }
    }
}
