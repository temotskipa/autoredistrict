package autoredistrict.data;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rook adjacency for a polygon coverage. RDH tract shapes come from Census TIGER, so neighbouring
 * tracts share exact boundary vertices: shared boundary segments are matched by hashing their
 * endpoints snapped to 1e-7 degrees (about 1 cm), and the shared length is accumulated per pair
 * in projected metres. Pairs the hashing misses (a vertex of one tract in the middle of the
 * other's edge) are found with an STRtree and an exact boundary intersection. Corner-only contact
 * isn't adjacency.
 */
public final class AdjacencyBuilder {
    
    /**
     * Shared boundaries shorter than this (metres) are treated as corner contact.
     */
    static final double MIN_SHARED_METRES = 1.0;
    private static final double SNAP = 1e7;
    
    private AdjacencyBuilder() {
    }
    
    /**
     * @param lonLat    geometries in geographic coordinates, as read from the shapefile
     * @param projected the same geometries projected to metres, vertex for vertex
     */
    public static Adjacency build(List<Geometry> lonLat, List<Geometry> projected) {
        int n = lonLat.size();
        long segments = 0;
        for (Geometry g : lonLat) {
            segments += g.getNumPoints();
        }
        SegmentTable table = new SegmentTable(segments);
        Map<Long, double[]> shared = new HashMap<>();
        double[] perimeter = new double[n];
        for (int i = 0; i < n; i++) {
            Geometry orig = lonLat.get(i);
            Geometry proj = projected.get(i);
            perimeter[i] = proj.getLength();
            long lastKey = Long.MIN_VALUE;
            double[] lastAcc = null;
            for (int p = 0; p < orig.getNumGeometries(); p++) {
                Polygon po = (Polygon) orig.getGeometryN(p);
                Polygon pp = (Polygon) proj.getGeometryN(p);
                for (int r = -1; r < po.getNumInteriorRing(); r++) {
                    LineString ro = r < 0 ? po.getExteriorRing() : po.getInteriorRingN(r);
                    LineString rp = r < 0 ? pp.getExteriorRing() : pp.getInteriorRingN(r);
                    Coordinate[] co = ro.getCoordinates();
                    Coordinate[] cp = rp.getCoordinates();
                    for (int j = 0; j + 1 < co.length; j++) {
                        long k1 = key(co[j]);
                        long k2 = key(co[j + 1]);
                        if (k1 == k2) {
                            continue;
                        }
                        int other = table.match(Math.min(k1, k2), Math.max(k1, k2), i);
                        if (other >= 0 && other != i) {
                            long pk = pairKey(other, i);
                            double[] acc;
                            if (pk == lastKey) {
                                acc = lastAcc;
                            }
                            else {
                                acc = shared.computeIfAbsent(pk, x -> new double[1]);
                                lastKey = pk;
                                lastAcc = acc;
                            }
                            acc[0] += cp[j].distance(cp[j + 1]);
                        }
                    }
                }
            }
        }
        int hashed = shared.size();
        int fallback = fallbackPairs(lonLat, projected, shared);
        List<Long> keys = new ArrayList<>(shared.keySet());
        keys.sort(null);
        List<int[]> pairs = new ArrayList<>();
        List<Double> lens = new ArrayList<>();
        for (long k : keys) {
            double len = shared.get(k)[0];
            if (len < MIN_SHARED_METRES) {
                continue;
            }
            pairs.add(new int[]{(int) (k >>> 32), (int) k});
            lens.add(len);
        }
        int m = pairs.size();
        int[] a = new int[m];
        int[] b = new int[m];
        double[] len = new double[m];
        double[] sharedSum = new double[n];
        for (int e = 0; e < m; e++) {
            a[e] = pairs.get(e)[0];
            b[e] = pairs.get(e)[1];
            len[e] = lens.get(e);
            sharedSum[a[e]] += len[e];
            sharedSum[b[e]] += len[e];
        }
        double[] outer = new double[n];
        for (int i = 0; i < n; i++) {
            outer[i] = Math.max(0, perimeter[i] - sharedSum[i]);
        }
        return new Adjacency(a, b, len, perimeter, outer, hashed, fallback);
    }
    
    private static int fallbackPairs(List<Geometry> lonLat, List<Geometry> projected, Map<Long, double[]> shared) {
        int n = lonLat.size();
        STRtree tree = new STRtree();
        for (int i = 0; i < n; i++) {
            tree.insert(lonLat.get(i).getEnvelopeInternal(), i);
        }
        tree.build();
        Geometry[] boundaries = new Geometry[n];
        int added = 0;
        for (int i = 0; i < n; i++) {
            Geometry gi = lonLat.get(i);
            PreparedGeometry prepared = null;
            @SuppressWarnings("unchecked") List<Integer> candidates = tree.query(gi.getEnvelopeInternal());
            for (int j : candidates) {
                if (j <= i || shared.containsKey(pairKey(i, j))) {
                    continue;
                }
                if (prepared == null) {
                    prepared = PreparedGeometryFactory.prepare(gi);
                }
                if (!prepared.intersects(lonLat.get(j))) {
                    continue;
                }
                if (boundaries[i] == null) {
                    boundaries[i] = gi.getBoundary();
                }
                if (boundaries[j] == null) {
                    boundaries[j] = lonLat.get(j).getBoundary();
                }
                double len = sharedLength(boundaries[i], boundaries[j], projected.get(i));
                if (len >= MIN_SHARED_METRES) {
                    shared.put(pairKey(i, j), new double[]{len});
                    added++;
                }
            }
        }
        return added;
    }
    
    /**
     * Length in metres of the overlap of two boundaries; 0 for corner contact.
     */
    private static double sharedLength(Geometry bi, Geometry bj, Geometry projectedI) {
        Geometry overlap;
        try {
            overlap = bi.intersection(bj);
        } catch (TopologyException e) {
            return 0;
        }
        if (overlap.getDimension() < 1) {
            return 0;
        }
        // Scale the degree length to metres with the local projected/geographic length ratio.
        double degLen = overlap.getLength();
        double ratio = projectedI.getLength() / Math.max(1e-12, bi.getLength());
        return degLen * ratio;
    }
    
    static long key(Coordinate c) {
        long lon = Math.round((c.x + 200.0) * SNAP);
        long lat = Math.round((c.y + 90.0) * SNAP);
        return (lon << 32) | (lat & 0xffffffffL);
    }
    
    static long pairKey(int i, int j) {
        int lo = Math.min(i, j);
        int hi = Math.max(i, j);
        return ((long) lo << 32) | (hi & 0xffffffffL);
    }
    
    /**
     * Undirected edges with shared lengths, plus each unit's perimeter and outer boundary.
     */
    public record Adjacency(int[] a, int[] b, double[] length, double[] perimeter, double[] outer, int hashedPairs, int fallbackPairs) {
        
        public int edgeCount() {
            return a.length;
        }
    }
    
    /**
     * Open-addressing hash of segments (two packed endpoints) to the first unit that had it.
     */
    private static final class SegmentTable {
        private final long[] ka;
        private final long[] kb;
        private final int[] owner;
        private final int mask;
        
        SegmentTable(long expected) {
            int cap = Integer.highestOneBit((int) Math.min(1L << 30, Math.max(16, expected * 2)) - 1) << 1;
            ka = new long[cap];
            kb = new long[cap];
            owner = new int[cap];
            java.util.Arrays.fill(owner, -1);
            mask = cap - 1;
        }
        
        /**
         * Records the segment for {@code unit}. Returns the other unit if the segment was already
         * recorded by a different one (consuming it), -1 if this is its first sighting.
         */
        int match(long p, long q, int unit) {
            long h = p * 0x9E3779B97F4A7C15L ^ q * 0xC2B2AE3D27D4EB4FL;
            int slot = (int) (h ^ (h >>> 31)) & mask;
            while (true) {
                int o = owner[slot];
                if (o == -1) {
                    ka[slot] = p;
                    kb[slot] = q;
                    owner[slot] = unit;
                    return -1;
                }
                if (ka[slot] == p && kb[slot] == q) {
                    if (o >= 0 && o != unit) {
                        owner[slot] = -2; // consumed: a segment borders at most two units
                        return o;
                    }
                    return -1;
                }
                slot = (slot + 1) & mask;
            }
        }
    }
}
