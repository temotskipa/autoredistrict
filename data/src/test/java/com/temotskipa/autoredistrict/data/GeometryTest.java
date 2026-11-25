package com.temotskipa.autoredistrict.data;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.densify.Densifier;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Projection, adjacency and connectivity on small fixtures with known answers.
 */
class GeometryTest {
    
    // GRS 1980, as in NAD83.
    private static final double A = 6378137.0;
    private static final double F = 1 / 298.257222101;
    private static final double E2 = 2 * F - F * F;
    private static final double E = Math.sqrt(E2);
    
    private static Map<Long, Double> edges(AdjacencyBuilder.Adjacency adj) {
        Map<Long, Double> out = new HashMap<>();
        for (int e = 0; e < adj.edgeCount(); e++) {
            out.put(AdjacencyBuilder.pairKey(adj.a()[e], adj.b()[e]), adj.length()[e]);
        }
        return out;
    }
    
    private static double q(double phi) {
        double s = Math.sin(phi);
        return (1 - E2) * (s / (1 - E2 * s * s) - (1 / (2 * E)) * Math.log((1 - E * s) / (1 + E * s)));
    }
    
    private static double m(double phi) {
        double s = Math.sin(phi);
        return Math.cos(phi) / Math.sqrt(1 - E2 * s * s);
    }
    
    /**
     * Snyder (1987), eqs. 14-1 to 14-6: ellipsoidal Albers equal-area conic.
     */
    private static double[] snyder(double lon, double lat, double lat1, double lat2, double lat0, double lon0) {
        double p1 = Math.toRadians(lat1);
        double p2 = Math.toRadians(lat2);
        double m1 = m(p1);
        double m2 = m(p2);
        double q1 = q(p1);
        double q2 = q(p2);
        double n = (m1 * m1 - m2 * m2) / (q2 - q1);
        double c = m1 * m1 + n * q1;
        double rho0 = A * Math.sqrt(c - n * q(Math.toRadians(lat0))) / n;
        double rho = A * Math.sqrt(c - n * q(Math.toRadians(lat))) / n;
        double theta = n * Math.toRadians(lon - lon0);
        return new double[]{rho * Math.sin(theta), rho0 - rho * Math.cos(theta)};
    }
    
    @Test
    void albersMatchesSnyderFormulasAtKnownPoints() {
        AlbersProjection p = AlbersProjection.of(43.5, 46.5, 45.0, -69.0, AlbersProjection.NAD83_ESRI);
        Coordinate origin = p.project(-69.0, 45.0);
        assertEquals(0.0, origin.x, 1e-6);
        assertEquals(0.0, origin.y, 1e-6);
        double[][] points = {{-70.5, 43.2}, {-67.1, 47.4}, {-69.0, 44.0}, {-71.0, 46.0}};
        for (double[] ll : points) {
            Coordinate c = p.project(ll[0], ll[1]);
            double[] expected = snyder(ll[0], ll[1], 43.5, 46.5, 45.0, -69.0);
            assertEquals(expected[0], c.x, 1e-3, "x at " + ll[0] + "," + ll[1]);
            assertEquals(expected[1], c.y, 1e-3, "y at " + ll[0] + "," + ll[1]);
            Coordinate back = p.toLonLat(c.x, c.y);
            assertEquals(ll[0], back.x, 1e-9);
            assertEquals(ll[1], back.y, 1e-9);
        }
    }
    
    @Test
    void albersIsEqualArea() {
        AlbersProjection p = AlbersProjection.of(43.5, 46.5, 45.0, -69.0, AlbersProjection.NAD83_ESRI);
        Geometry cell = Densifier.densify(Fixtures.box(-70, 44, -69, 45), 0.005);
        double projected = p.project(cell).getArea();
        double exact = A * A / 2 * Math.toRadians(1.0) * Math.abs(q(Math.toRadians(45)) - q(Math.toRadians(44)));
        assertEquals(exact, projected, exact * 1e-6);
    }
    
    @Test
    void aleutiansLandWestOfTheRestOfAlaska() {
        List<Geometry> geoms = List.of(Fixtures.box(172.5, 52.5, 173.0, 53.0), Fixtures.box(-150, 60, -149, 61), Fixtures.box(-179.5, 51.0, -179.0, 51.5));
        Envelope env = AlbersProjection.unwrappedBounds(geoms);
        assertEquals(-187.5, env.getMinX(), 1e-9);
        assertEquals(-149.0, env.getMaxX(), 1e-9);
        AlbersProjection p = AlbersProjection.forBounds(env, AlbersProjection.NAD83_ESRI);
        double attu = p.project(172.75, 52.75).x;
        double adak = p.project(-179.25, 51.25).x;
        double anchorage = p.project(-149.5, 60.5).x;
        assertTrue(attu < adak && adak < anchorage, attu + " " + adak + " " + anchorage);
        // Attu is about 8 degrees west of Adak: several hundred km, not across the globe.
        assertTrue(adak - attu > 400_000 && adak - attu < 700_000, "gap " + (adak - attu));
    }
    
    @Test
    void lonLatExportCutsPolygonsAtTheAntimeridian() {
        List<Geometry> alaska = List.of(Fixtures.box(172.5, 52.5, 173.0, 53.0), Fixtures.box(-150, 60, -149, 61));
        AlbersProjection p = AlbersProjection.forBounds(AlbersProjection.unwrappedBounds(alaska), AlbersProjection.NAD83_ESRI);
        // A district from 178 E to 176 W (unwrapped: -182 to -176), like the western Aleutians.
        Geometry district = p.project(Densifier.densify(Fixtures.box(-182, 51, -176, 53), 0.05));
        Geometry ll = p.toLonLat(district);
        Envelope env = ll.getEnvelopeInternal();
        assertTrue(env.getMinX() >= -180 && env.getMaxX() <= 180, "within [-180, 180]: " + env);
        assertEquals(2, ll.getNumGeometries());
        for (int i = 0; i < ll.getNumGeometries(); i++) {
            Envelope part = ll.getGeometryN(i).getEnvelopeInternal();
            assertTrue(part.getWidth() < 10, "no part spans the globe: " + part);
        }
        assertEquals(12.0, ll.getArea(), 1e-6, "the two parts keep the district's area in square degrees");
        // A district that doesn't cross stays one polygon in place.
        Geometry mainland = p.toLonLat(p.project(Fixtures.box(-150, 60, -149, 61)));
        assertEquals(1, mainland.getNumGeometries());
        assertEquals(-150.0, mainland.getEnvelopeInternal().getMinX(), 1e-6);
    }

    @Test
    void rookAdjacencyOnGridHasKnownSharedEdgesAndNoCorners() {
        List<Geometry> cells = new ArrayList<>();
        for (int r = 0; r < 3; r++) {
            for (int c = 0; c < 3; c++) {
                cells.add(Fixtures.box(c * 10, r * 10, c * 10 + 10, r * 10 + 10));
            }
        }
        AdjacencyBuilder.Adjacency adj = AdjacencyBuilder.build(cells, cells);
        assertEquals(12, adj.edgeCount());
        Map<Long, Double> lengths = edges(adj);
        assertEquals(10.0, lengths.get(AdjacencyBuilder.pairKey(0, 1)), 1e-9);
        assertEquals(10.0, lengths.get(AdjacencyBuilder.pairKey(4, 7)), 1e-9);
        assertTrue(!lengths.containsKey(AdjacencyBuilder.pairKey(0, 4)), "diagonal corner contact isn't adjacency");
        assertEquals(40.0, adj.perimeter()[4], 1e-9);
        assertEquals(0.0, adj.outer()[4], 1e-9);
        assertEquals(20.0, adj.outer()[0], 1e-9);
        assertEquals(0, adj.fallbackPairs());
    }
    
    @Test
    void fallbackFindsTJunctionsTheHashMisses() {
        // A's top edge is one segment; B and C split it at x=10, a vertex A doesn't have.
        Geometry a = Fixtures.poly(0, 0, 20, 0, 20, 10, 0, 10);
        Geometry b = Fixtures.box(0, 10, 10, 20);
        Geometry c = Fixtures.box(10, 10, 20, 20);
        AdjacencyBuilder.Adjacency adj = AdjacencyBuilder.build(List.of(a, b, c), List.of(a, b, c));
        Map<Long, Double> lengths = edges(adj);
        assertEquals(3, adj.edgeCount());
        assertEquals(2, adj.fallbackPairs());
        assertEquals(10.0, lengths.get(AdjacencyBuilder.pairKey(0, 1)), 1e-9);
        assertEquals(10.0, lengths.get(AdjacencyBuilder.pairKey(0, 2)), 1e-9);
        assertEquals(10.0, lengths.get(AdjacencyBuilder.pairKey(1, 2)), 1e-9);
    }

    @Test
    void aBorderThatIsOnlyPartlyHashedIsMeasuredInFull() {
        // A and B share the top edge of A, 20 long. From x=10 to 20 both have the same segment;
        // from 0 to 10 B has an extra vertex at x=5, 4 mm off A's edge, so nothing there hashes.
        Geometry a = Fixtures.poly(0, 0, 20, 0, 20, 10, 10, 10, 0, 10);
        Geometry b = Fixtures.poly(0, 10, 5, 10.004, 10, 10, 20, 10, 20, 20, 0, 20);
        AdjacencyBuilder.Adjacency adj = AdjacencyBuilder.build(List.of(a, b), List.of(a, b));
        assertEquals(1, adj.edgeCount());
        assertEquals(1, adj.hashedPairs());
        assertEquals(1, adj.fallbackPairs(), "the unhashed half is counted");
        assertEquals(20.0, edges(adj).get(AdjacencyBuilder.pairKey(0, 1)), 1e-5);
        assertEquals(40.0, adj.outer()[0], 1e-5, "A's outer boundary is its bottom and sides");
        // A neighbour 1 m away across a gap shares nothing.
        Geometry c = Fixtures.box(0, -11, 20, -1);
        AdjacencyBuilder.Adjacency gap = AdjacencyBuilder.build(List.of(a, c), List.of(a, c));
        assertEquals(0, gap.edgeCount());
    }

    @Test
    void islandsAreLinkedToTheirNearestNeighbour() {
        List<Geometry> geoms = List.of(Fixtures.box(0, 0, 10, 10), Fixtures.box(10, 0, 20, 10), Fixtures.box(50, 0, 60, 10), Fixtures.box(26, 30, 30, 34), Fixtures.box(80, 80, 90, 90));
        AdjacencyBuilder.Adjacency adj = AdjacencyBuilder.build(geoms, geoms);
        assertEquals(1, adj.edgeCount());
        Connectivity.Repair repair = Connectivity.connect(geoms, adj.a(), adj.b());
        assertEquals(4, repair.componentsBefore());
        assertEquals(3, repair.bridges().size());
        Connectivity.Bridge first = repair.bridges().stream().filter(br -> br.a() == 1 || br.b() == 1).findFirst().orElseThrow();
        assertTrue(first.a() == 1 || first.b() == 1);
        // The graph with bridges is connected.
        Connectivity.UnionFind uf = new Connectivity.UnionFind(geoms.size());
        uf.union(adj.a()[0], adj.b()[0]);
        repair.bridges().forEach(br -> uf.union(br.a(), br.b()));
        assertEquals(1, uf.count());
        assertTrue(new Connectivity.Bridge(1, 2, 30.0) == new Connectivity.Bridge(1, 2, 30.0));
    }
}
