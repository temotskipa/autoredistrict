package com.temotskipa.autoredistrict.data;

import java.util.Locale;

/**
 * One of the 50 states: postal abbreviation, 2-digit FIPS code, name, and its House seats under
 * the official 2020 apportionment. A value object.
 */
public value record StateInfo(String abbr, String fips, String name, int seats) {
    
    /**
     * Lower-case abbreviation, as used in RDH file names ({@code me_pl2020_t.zip}).
     */
    public String key() {
        return abbr.toLowerCase(Locale.ROOT);
    }
    
    public boolean atLarge() {
        return seats == 1;
    }
}
