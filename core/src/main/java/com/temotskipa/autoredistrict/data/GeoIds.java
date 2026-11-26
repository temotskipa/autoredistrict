package com.temotskipa.autoredistrict.data;

import java.util.Optional;

/**
 * Census GEOID handling. GEOIDs are strings; a CSV reader that treats them as numbers drops the
 * leading zero of state FIPS codes below 10 (Alabama's {@code 01...}), and may add {@code .0}.
 */
public final class GeoIds {
    
    public static final int TRACT_WIDTH = 11;
    public static final int BLOCK_WIDTH = 15;
    
    private GeoIds() {
    }
    
    /**
     * Strips whitespace, quotes and a trailing {@code .0}; doesn't pad.
     */
    public static String clean(String raw) {
        if (raw == null) {
            throw new DataException("Some rows have no GEOID.");
        }
        String s = raw.strip();
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1).strip();
        }
        if (s.endsWith(".0")) {
            s = s.substring(0, s.length() - 2);
        }
        if (s.isEmpty()) {
            throw new DataException("Some rows have no GEOID.");
        }
        return s;
    }
    
    /**
     * Zero-pads a GEOID that is one digit short of the tract or block width, which is what a
     * numeric read of a state FIPS below 10 produces. Other widths are returned unchanged.
     */
    public static String normalize(String raw) {
        String s = clean(raw);
        if (s.length() == TRACT_WIDTH - 1 || s.length() == BLOCK_WIDTH - 1) {
            return "0" + s;
        }
        return s;
    }
    
    /**
     * Pads a block GEOID to 15 characters; fails on anything else.
     */
    public static String block(String raw, String source) {
        String s = normalize(raw);
        if (s.length() != BLOCK_WIDTH || !s.chars().allMatch(Character::isDigit)) {
            throw new DataException(source + " has a block GEOID20 '" + raw + "' that isn't a 14- or 15-digit block code.");
        }
        return s;
    }
    
    /**
     * Whether a tract GEOID is a water-only tract, which the Census Bureau numbers 9900 to 9998
     * (the first four digits of the six-digit tract code).
     */
    public static boolean isWaterTract(String geoid) {
        if (geoid.length() != TRACT_WIDTH || !geoid.chars().allMatch(Character::isDigit)) {
            return false;
        }
        int base = Integer.parseInt(geoid.substring(5, 9));
        return base >= 9900 && base <= 9998;
    }
    
    /**
     * "tract" for 11 digits, "block" for 15, empty otherwise.
     */
    public static Optional<String> resolutionForWidth(int width) {
        return switch (width) {
            case TRACT_WIDTH -> Optional.of("tract");
            case BLOCK_WIDTH -> Optional.of("block");
            default -> Optional.empty();
        };
    }
}
