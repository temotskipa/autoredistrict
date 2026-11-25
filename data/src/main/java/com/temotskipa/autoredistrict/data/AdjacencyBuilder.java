package com.temotskipa.autoredistrict.data;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rook adjacency for a polygon coverage. RDH tract shapes come from Census TIGER, so neighbouring
 * tracts share exact boundary vertices: shared boundary segments are matched by hashing their
 * endpoints snapped to 1e-7 degrees (about 1 cm), and the shared length is accumulated per pair
 * in projected metres. Borders the hash misses, in whole or in part (a vertex of one tract in the
 * middle of the other's edge), are measured from the segments neither tract matched. Corner-only
 * contact isn't adjacency.
 */
public final class AdjacencyBuilder {

    /**
     * Shared boundaries shorter than this (metres) are treated as corner contact.
     */
    static final double MIN_SHARED_METRES = 1.0;
    /**
     * Unmatched segments of two units that stay this close (metres) along a stretch share that stretch.
     */
    static final double OVERLAP_TOLERANCE_METRES = 0.05;
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
            perimeter[i] = projected.get(i).getLength();
            List<Coordinate[]> ro = rings(lonLat.get(i));
            List<Coordinate[]> rp = rings(projected.get(i));
            long lastKey = Long.MIN_VALUE;
            double[] lastAcc = null;
            for (int r = 0; r < ro.size(); r++) {
                Coordinate[] co = ro.get(r);
                Coordinate[] cp = rp.get(r);
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
        int hashed = shared.size();
        int unhashed = addUnhashedBorders(lonLat, projected, table, shared);
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
        return new Adjacency(a, b, len, perimeter, outer, hashed, unhashed);
    }

    /**
     * Adds the borders the segment hash missed: stretches where one unit has a vertex the other
     * lacks (a T-junction), whether they are a pair's whole border or only part of it. Neither
     * unit matched its segments along such a stretch, so the stretch is where their unmatched
     * segments overlap, compared in projected metres to within {@link #OVERLAP_TOLERANCE_METRES}.
     * Matched segments take no part, so no length is counted twice. Most unmatched segments are
     * the state's outer boundary, which no other unit shares.
     *
     * @return the number of pairs that gained at least {@link #MIN_SHARED_METRES}
     */
    private static int addUnhashedBorders(List<Geometry> lonLat, List<Geometry> projected, SegmentTable table, Map<Long, double[]> shared) {
        Unmatched un = new Unmatched();
        for (int i = 0; i < lonLat.size(); i++) {
            List<Coordinate[]> ro = rings(lonLat.get(i));
            List<Coordinate[]> rp = rings(projected.get(i));
            for (int r = 0; r < ro.size(); r++) {
                Coordinate[] co = ro.get(r);
                Coordinate[] cp = rp.get(r);
                for (int j = 0; j + 1 < co.length; j++) {
                    long k1 = key(co[j]);
                    long k2 = key(co[j + 1]);
                    if (k1 != k2 && table.owner(Math.min(k1, k2), Math.max(k1, k2)) == i) {
                        un.add(i, cp[j], cp[j + 1]);
                    }
                }
            }
        }
        double tol = OVERLAP_TOLERANCE_METRES;
        STRtree tree = new STRtree();
        for (int s = 0; s < un.size; s++) {
            tree.insert(un.envelope(s, tol), s);
        }
        tree.build();
        Map<Long, double[]> gained = new HashMap<>();
        for (int s = 0; s < un.size; s++) {
            int us = un.unit[s];
            @SuppressWarnings("unchecked") List<Integer> near = tree.query(un.envelope(s, tol));
            for (int t : near) {
                int ut = un.unit[t];
                if (ut <= us) {
                    continue;
                }
                double len = un.overlap(s, t, tol);
                if (len > 0) {
                    long pk = pairKey(us, ut);
                    shared.computeIfAbsent(pk, x -> new double[1])[0] += len;
                    gained.computeIfAbsent(pk, x -> new double[1])[0] += len;
                }
            }
        }
        int count = 0;
        for (double[] g : gained.values()) {
            if (g[0] >= MIN_SHARED_METRES) {
                count++;
            }
        }
        return count;
    }

    /**
     * Exterior and interior rings of each polygon, in order, so that the rings of a geometry and of
     * its vertex-for-vertex projection line up.
     */
    private static List<Coordinate[]> rings(Geometry g) {
        List<Coordinate[]> out = new ArrayList<>();
        for (int p = 0; p < g.getNumGeometries(); p++) {
            Polygon poly = (Polygon) g.getGeometryN(p);
            out.add(poly.getExteriorRing().getCoordinates());
            for (int r = 0; r < poly.getNumInteriorRing(); r++) {
                out.add(poly.getInteriorRingN(r).getCoordinates());
            }
        }
        return out;
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
     *
     * @param hashedPairs   pairs with at least one segment matched by the hash
     * @param fallbackPairs pairs whose border the hash missed by at least {@link #MIN_SHARED_METRES}, in whole or in part
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
            Arrays.fill(owner, -1);
            mask = cap - 1;
        }

        private int slot(long p, long q) {
            long h = p * 0x9E3779B97F4A7C15L ^ q * 0xC2B2AE3D27D4EB4FL;
            return (int) (h ^ (h >>> 31)) & mask;
        }

        /**
         * Records the segment for {@code unit}. Returns the other unit if the segment was already
         * recorded by a different one (consuming it), -1 if this is its first sighting.
         */
        int match(long p, long q, int unit) {
            int slot = slot(p, q);
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

        /**
         * The unit that recorded the segment if no other unit matched it, -2 if one did, -1 if it
         * was never recorded.
         */
        int owner(long p, long q) {
            int slot = slot(p, q);
            while (true) {
                int o = owner[slot];
                if (o == -1 || (ka[slot] == p && kb[slot] == q)) {
                    return o;
                }
                slot = (slot + 1) & mask;
            }
        }
    }

    /**
     * Segments no other unit matched, in projected metres, with the unit each belongs to.
     */
    private static final class Unmatched {
        private double[] x1 = new double[1024];
        private double[] y1 = new double[1024];
        private double[] x2 = new double[1024];
        private double[] y2 = new double[1024];
        private int[] unit = new int[1024];
        private int size;

        void add(int u, Coordinate p, Coordinate q) {
            if (size == unit.length) {
                int cap = size * 2;
                x1 = Arrays.copyOf(x1, cap);
                y1 = Arrays.copyOf(y1, cap);
                x2 = Arrays.copyOf(x2, cap);
                y2 = Arrays.copyOf(y2, cap);
                unit = Arrays.copyOf(unit, cap);
            }
            x1[size] = p.x;
            y1[size] = p.y;
            x2[size] = q.x;
            y2[size] = q.y;
            unit[size++] = u;
        }

        Envelope envelope(int s, double pad) {
            Envelope e = new Envelope(x1[s], x2[s], y1[s], y2[s]);
            e.expandBy(pad);
            return e;
        }

        /**
         * Length of the stretch along which segments s and t lie within {@code tol} of each other.
         */
        double overlap(int s, int t, double tol) {
            return Math.max(along(s, t, tol), along(t, s, tol));
        }

        /**
         * How much of segment s lies under t, if both ends of t are within {@code tol} of s's line.
         */
        private double along(int s, int t, double tol) {
            double ux = x2[s] - x1[s];
            double uy = y2[s] - y1[s];
            double len2 = ux * ux + uy * uy;
            if (len2 == 0) {
                return 0;
            }
            double len = Math.sqrt(len2);
            double cx = x1[t] - x1[s];
            double cy = y1[t] - y1[s];
            double dx = x2[t] - x1[s];
            double dy = y2[t] - y1[s];
            if (Math.abs(ux * cy - uy * cx) > tol * len || Math.abs(ux * dy - uy * dx) > tol * len) {
                return 0;
            }
            double tc = (cx * ux + cy * uy) / len2;
            double td = (dx * ux + dy * uy) / len2;
            double lo = Math.max(0, Math.min(tc, td));
            double hi = Math.min(1, Math.max(tc, td));
            return hi > lo ? (hi - lo) * len : 0;
        }
    }
}
