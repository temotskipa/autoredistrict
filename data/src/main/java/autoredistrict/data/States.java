package autoredistrict.data;

import java.util.*;

/**
 * The 50 states and the official 2020 Census House apportionment, announced on April 26, 2021
 * and effective for the 2022 elections. DC and the territories are out of scope.
 */
public final class States {
    
    private static final List<StateInfo> ALL = List.of(new StateInfo("AL", "01", "Alabama", 7), new StateInfo("AK", "02", "Alaska", 1), new StateInfo("AZ", "04", "Arizona", 9), new StateInfo("AR", "05", "Arkansas", 4), new StateInfo("CA", "06", "California", 52), new StateInfo("CO", "08", "Colorado", 8), new StateInfo("CT", "09", "Connecticut", 5), new StateInfo("DE", "10", "Delaware", 1), new StateInfo("FL", "12", "Florida", 28), new StateInfo("GA", "13", "Georgia", 14), new StateInfo("HI", "15", "Hawaii", 2), new StateInfo("ID", "16", "Idaho", 2), new StateInfo("IL", "17", "Illinois", 17), new StateInfo("IN", "18", "Indiana", 9), new StateInfo("IA", "19", "Iowa", 4), new StateInfo("KS", "20", "Kansas", 4), new StateInfo("KY", "21", "Kentucky", 6), new StateInfo("LA", "22", "Louisiana", 6), new StateInfo("ME", "23", "Maine", 2), new StateInfo("MD", "24", "Maryland", 8), new StateInfo("MA", "25", "Massachusetts", 9), new StateInfo("MI", "26", "Michigan", 13), new StateInfo("MN", "27", "Minnesota", 8), new StateInfo("MS", "28", "Mississippi", 4), new StateInfo("MO", "29", "Missouri", 8), new StateInfo("MT", "30", "Montana", 2), new StateInfo("NE", "31", "Nebraska", 3), new StateInfo("NV", "32", "Nevada", 4), new StateInfo("NH", "33", "New Hampshire", 2), new StateInfo("NJ", "34", "New Jersey", 12), new StateInfo("NM", "35", "New Mexico", 3), new StateInfo("NY", "36", "New York", 26), new StateInfo("NC", "37", "North Carolina", 14), new StateInfo("ND", "38", "North Dakota", 1), new StateInfo("OH", "39", "Ohio", 15), new StateInfo("OK", "40", "Oklahoma", 5), new StateInfo("OR", "41", "Oregon", 6), new StateInfo("PA", "42", "Pennsylvania", 17), new StateInfo("RI", "44", "Rhode Island", 2), new StateInfo("SC", "45", "South Carolina", 7), new StateInfo("SD", "46", "South Dakota", 1), new StateInfo("TN", "47", "Tennessee", 9), new StateInfo("TX", "48", "Texas", 38), new StateInfo("UT", "49", "Utah", 4), new StateInfo("VT", "50", "Vermont", 1), new StateInfo("VA", "51", "Virginia", 11), new StateInfo("WA", "53", "Washington", 10), new StateInfo("WV", "54", "West Virginia", 2), new StateInfo("WI", "55", "Wisconsin", 8), new StateInfo("WY", "56", "Wyoming", 1));
    /**
     * Lookup tables, built on first use (a JDK 28 preview lazy constant).
     */
    private static final LazyConstant<Map<String, StateInfo>> BY_KEY = LazyConstant.of(() -> {
        Map<String, StateInfo> map = new LinkedHashMap<>();
        for (StateInfo s : ALL) {
            map.put(s.abbr(), s);
            map.put(s.fips(), s);
        }
        return Map.copyOf(map);
    });
    
    private States() {
    }
    
    /**
     * All 50 states in alphabetical order of name.
     */
    public static List<StateInfo> all() {
        return ALL;
    }
    
    /**
     * Finds a state by postal abbreviation (any case) or 1–2 digit FIPS code.
     */
    public static Optional<StateInfo> find(String abbrOrFips) {
        if (abbrOrFips == null) {
            return Optional.empty();
        }
        String key = abbrOrFips.strip().toUpperCase(Locale.ROOT);
        if (key.isEmpty()) {
            return Optional.empty();
        }
        if (key.chars().allMatch(Character::isDigit) && key.length() < 2) {
            key = "0" + key;
        }
        return Optional.ofNullable(BY_KEY.get().get(key));
    }
    
    /**
     * Like {@link #find(String)} but throws a {@link DataException} naming the bad input.
     */
    public static StateInfo require(String abbrOrFips) {
        return find(abbrOrFips).orElseThrow(() -> new DataException("Unknown state '" + abbrOrFips + "'. Use a postal abbreviation like ME or a FIPS code like 23."));
    }
    
    /**
     * Official 2020 House seats for a 2-digit state FIPS code, or empty outside the 50 states.
     */
    public static OptionalInt apportionedSeats(String fips) {
        if (fips == null || fips.isBlank() || !fips.strip().chars().allMatch(Character::isDigit)) {
            return OptionalInt.empty();
        }
        StateInfo s = BY_KEY.get().get(fips.strip().length() < 2 ? "0" + fips.strip() : fips.strip());
        return s == null ? OptionalInt.empty() : OptionalInt.of(s.seats());
    }
    
    /**
     * FIPS code to seats, for all 50 states.
     */
    public static Map<String, Integer> apportionment2020() {
        Map<String, Integer> map = new LinkedHashMap<>();
        for (StateInfo s : ALL) {
            map.put(s.fips(), s.seats());
        }
        return map;
    }
}
