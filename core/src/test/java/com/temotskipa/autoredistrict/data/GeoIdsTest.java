package com.temotskipa.autoredistrict.data;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeoIdsTest {
    
    @Test
    void waterOnlyTractsAreNumbered9900To9998() {
        assertTrue(GeoIds.isWaterTract("12086990000"));
        assertTrue(GeoIds.isWaterTract("12087990100"));
        assertTrue(GeoIds.isWaterTract("37019999800"));
        assertFalse(GeoIds.isWaterTract("12011110403"));
        assertFalse(GeoIds.isWaterTract("12086989900"));
        assertFalse(GeoIds.isWaterTract("12086999900"));
        // Blocks and malformed ids are not water tracts.
        assertFalse(GeoIds.isWaterTract("120869900001000"));
        assertFalse(GeoIds.isWaterTract("1208699000X"));
    }
}
