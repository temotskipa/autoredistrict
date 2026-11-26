package com.temotskipa.autoredistrict.data;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@code manifest.json} of one imported state in the cache.
 */
public value record Manifest(int format, String state, String fips, String name, int seats, String source, List<InputFile> inputFiles, List<String> memberFiles, String importedAt, int tracts, long population, int edges, int hashedPairs, int fallbackPairs, int componentsBefore, List<BridgeInfo> bridges, ProjectionInfo projection, double simplifyToleranceMetres, Map<String, VoteInfo> votes, List<String> warnings) {
    
    public static final int FORMAT = 1;
    
    public Manifest withVotes(String election, VoteInfo info) {
        Map<String, VoteInfo> v = new TreeMap<>(votes == null ? Map.of() : votes);
        v.put(election, info);
        return new Manifest(format, state, fips, name, seats, source, inputFiles, memberFiles, importedAt, tracts, population, edges, hashedPairs, fallbackPairs, componentsBefore, bridges, projection, simplifyToleranceMetres, v, warnings);
    }
    
    /**
     * An input file with its SHA-256.
     */
    public value record InputFile(String name, String sha256, long bytes) {
    }
    
    /**
     * A bridge edge, by GEOID.
     */
    public value record BridgeInfo(String a, String b, double gapMetres) {
    }
    
    /**
     * The state's Albers parameters.
     */
    public value record ProjectionInfo(double lat1, double lat2, double lat0, double lon0, String geographicWkt, String wkt) {
        
        public AlbersProjection toProjection() {
            return AlbersProjection.of(lat1, lat2, lat0, lon0, geographicWkt);
        }
    }
    
    /**
     * Votes cached for one election.
     */
    public value record VoteInfo(String election, String source, String sha256, double dem, double rep, double demShare, long blocks, long blocksOutside, long tractsMissing, Double totPop, String warning) {
    }
}
