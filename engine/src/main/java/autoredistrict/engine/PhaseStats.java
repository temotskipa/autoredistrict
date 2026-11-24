package autoredistrict.engine;

/**
 * What one phase of one chain did. A value object.
 */
public value record PhaseStats(String name, long steps, long accepted, double startTemperature, double endTemperature, double seconds) {
}
