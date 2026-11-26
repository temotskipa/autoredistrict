package com.temotskipa.autoredistrict.service;

/**
 * One district of a plan. A value object.
 *
 * @param deviation    (population - ideal) / ideal
 * @param demShare     D / (D + R)
 * @param margin       (D - R) / (D + R) in points; positive means a D win
 * @param polsbyPopper 4 pi area / perimeter^2
 */
public value record DistrictResult(int district, long population, double deviation, double dem, double rep, double demShare, String winner, double margin, int tracts, double areaKm2, double polsbyPopper, boolean contiguous) {
    
    /**
     * R / (D + R), the two-party complement of {@link #demShare()}.
     */
    public double repShare() {
        return 1 - demShare;
    }
}
