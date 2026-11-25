package com.temotskipa.autoredistrict.service;

import com.temotskipa.autoredistrict.data.AlbersProjection;
import com.temotskipa.autoredistrict.data.BlockVotes.Election;
import com.temotskipa.autoredistrict.data.Manifest;
import com.temotskipa.autoredistrict.data.StateInfo;
import com.temotskipa.autoredistrict.data.TractCache;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Writes a small synthetic state (a grid of 1 km tracts) straight into a cache folder.
 */
final class SyntheticState {
    
    private static final int POP_PER_TRACT = 1000;
    
    private SyntheticState() {
    }
    
    /**
     * A w x h grid of 1,000-person tracts; D share falls from 70% at the left to 30% at the right.
     */
    static void write(Path cache, StateInfo st, int w, int h) {
        write(cache, st, w, h, 0.0);
    }
    
    /**
     * As above with every D share shifted by {@code shift}.
     */
    static void write(Path cache, StateInfo st, int w, int h, double shift) {
        int n = w * h;
        GeometryFactory gf = new GeometryFactory();
        TractCache.Tracts t = new TractCache.Tracts(new String[n], new String[n], new int[n], new double[n], new double[n], new double[n], new double[n], new double[n]);
        List<Geometry> geoms = new ArrayList<>();
        double[] dem = new double[n];
        double[] rep = new double[n];
        List<int[]> edges = new ArrayList<>();
        for (int r = 0; r < h; r++) {
            for (int c = 0; c < w; c++) {
                int i = r * w + c;
                t.geoid()[i] = String.format("%s001%06d", st.fips(), i + 1);
                t.name()[i] = "Tract " + (i + 1);
                t.pop()[i] = POP_PER_TRACT;
                t.area()[i] = 1e6;
                t.x()[i] = c * 1000 + 500;
                t.y()[i] = r * 1000 + 500;
                t.perimeter()[i] = 4000;
                t.outer()[i] = 1000.0 * ((c == 0 ? 1 : 0) + (c == w - 1 ? 1 : 0) + (r == 0 ? 1 : 0) + (r == h - 1 ? 1 : 0));
                geoms.add(gf.createPolygon(new Coordinate[]{new Coordinate(c * 1000, r * 1000), new Coordinate(c * 1000 + 1000, r * 1000), new Coordinate(c * 1000 + 1000, r * 1000 + 1000), new Coordinate(c * 1000, r * 1000 + 1000), new Coordinate(c * 1000, r * 1000)}));
                double share = (w == 1 ? 0.6 : 0.7 - 0.4 * c / (w - 1.0)) + shift;
                dem[i] = 100 * share;
                rep[i] = 100 * (1 - share);
                if (c + 1 < w) {
                    edges.add(new int[]{i, i + 1});
                }
                if (r + 1 < h) {
                    edges.add(new int[]{i, i + w});
                }
            }
        }
        TractCache.Edges e = new TractCache.Edges(new int[edges.size()], new int[edges.size()], new double[edges.size()], new boolean[edges.size()]);
        for (int j = 0; j < edges.size(); j++) {
            e.a()[j] = edges.get(j)[0];
            e.b()[j] = edges.get(j)[1];
            e.length()[j] = 1000;
        }
        AlbersProjection p = AlbersProjection.of(44, 46, 45, -69, AlbersProjection.NAD83_ESRI);
        double d = 0;
        double rr = 0;
        for (int i = 0; i < n; i++) {
            d += dem[i];
            rr += rep[i];
        }
        Manifest m = new Manifest(Manifest.FORMAT, st.abbr(), st.fips(), st.name(), st.seats(), "synthetic test", List.of(), List.of(), "2026-11-24T00:00:00Z", n, (long) n * POP_PER_TRACT, edges.size(), edges.size(), 0, 1, List.of(), new Manifest.ProjectionInfo(44, 46, 45, -69, p.geographicWkt(), p.wkt()), 25, Map.of("2024", new Manifest.VoteInfo("2024", "synthetic", "", d, rr, d / (d + rr), n, 0, 0, null, null)), List.of());
        TractCache.write(cache, st, m, t, e, geoms, Election.Y2024, dem, rep);
    }
}
