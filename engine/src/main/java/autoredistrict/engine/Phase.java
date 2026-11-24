package autoredistrict.engine;

/**
 * One phase of the schedule: {@code steps} proposals with these weights, cooled exponentially
 * from a temperature at which a median uphill move (scored without the population term) is
 * accepted with probability {@code startAcceptance}, down to {@code coolTo} times that.
 * {@code selectionBias} is the surface-tension exponent for choosing boundary units.
 */
public value record Phase(String name, long steps, Weights weights, double startAcceptance, double coolTo, double selectionBias) {
}
