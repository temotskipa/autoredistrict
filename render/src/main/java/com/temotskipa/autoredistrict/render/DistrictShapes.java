package com.temotskipa.autoredistrict.render;

import org.locationtech.jts.coverage.CoverageUnion;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.operation.union.UnaryUnionOp;

import java.util.ArrayList;
import java.util.List;

/**
 * District polygons from tract polygons. The tract geometry is a coverage (simplified as one at
 * import), so a coverage union, which only drops shared edges, is fast and exact; a general union
 * is the fallback.
 */
public final class DistrictShapes {
    
    private DistrictShapes() {
    }
    
    public static Geometry[] of(List<Geometry> tracts, int[] assignment, int k) {
        List<List<Geometry>> parts = new ArrayList<>(k);
        for (int d = 0; d < k; d++) {
            parts.add(new ArrayList<>());
        }
        for (int i = 0; i < assignment.length; i++) {
            parts.get(assignment[i]).add(tracts.get(i));
        }
        Geometry[] out = new Geometry[k];
        GeometryFactory gf = tracts.isEmpty() ? new GeometryFactory() : tracts.getFirst().getFactory();
        for (int d = 0; d < k; d++) {
            List<Geometry> p = parts.get(d);
            if (p.isEmpty()) {
                out[d] = gf.createPolygon();
                continue;
            }
            Geometry u;
            try {
                u = CoverageUnion.union(p.toArray(Geometry[]::new));
            } catch (RuntimeException e) {
                u = null;
            }
            if (u == null || u.isEmpty()) {
                try {
                    u = UnaryUnionOp.union(p);
                } catch (TopologyException e) {
                    u = gf.buildGeometry(p).buffer(0);
                }
            }
            out[d] = u;
        }
        return out;
    }
}
