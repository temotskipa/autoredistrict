package com.temotskipa.autoredistrict.data;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ported from the Python tests/test_apportionment.py.
 */
class StatesTest {
    
    @Test
    void apportionment2020Has50StatesSummingTo435() {
        Map<String, Integer> table = States.apportionment2020();
        assertEquals(50, table.size());
        assertEquals(435, table.values().stream().mapToInt(Integer::intValue).sum());
        assertTrue(table.keySet().stream().allMatch(k -> k.length() == 2 && k.chars().allMatch(Character::isDigit)));
        assertTrue(table.values().stream().allMatch(s -> s >= 1));
        assertEquals(6, States.all().stream().filter(StateInfo::atLarge).count(), "AK, DE, ND, SD, VT, WY");
    }
    
    @Test
    void returnsOfficial2020Counts() {
        assertEquals(OptionalInt.of(2), States.apportionedSeats("30")); // Montana
        assertEquals(OptionalInt.of(2), States.apportionedSeats("44")); // Rhode Island
        assertEquals(OptionalInt.of(7), States.apportionedSeats("01")); // Alabama
        assertEquals(OptionalInt.of(1), States.apportionedSeats("56")); // Wyoming
        assertEquals(OptionalInt.of(52), States.apportionedSeats("06")); // California
        assertEquals(OptionalInt.of(38), States.apportionedSeats("48")); // Texas
        assertEquals(OptionalInt.of(7), States.apportionedSeats("1"));
    }
    
    @Test
    void outsideTheTableIsEmpty() {
        assertEquals(OptionalInt.empty(), States.apportionedSeats("11")); // DC
        assertEquals(OptionalInt.empty(), States.apportionedSeats("72")); // Puerto Rico
        assertEquals(OptionalInt.empty(), States.apportionedSeats("00"));
        assertEquals(OptionalInt.empty(), States.apportionedSeats(null));
        assertEquals(OptionalInt.empty(), States.apportionedSeats(""));
    }
    
    @Test
    void findsByAbbreviationOrFips() {
        assertEquals("23", States.require("me").fips());
        assertEquals("NC", States.require("37").abbr());
        assertTrue(States.find("DC").isEmpty());
        assertEquals(new StateInfo("ME", "23", "Maine", 2), States.require("ME"));
    }
    
    @Test
    void geoidsArePaddedOnlyWhenOneShort() {
        assertEquals("01001020100", GeoIds.normalize("1001020100"));
        assertEquals("010010201001000", GeoIds.normalize("10010201001000.0"));
        assertEquals("23001", GeoIds.normalize(" 23001 "));
    }
}
