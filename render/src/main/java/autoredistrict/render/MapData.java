package autoredistrict.render;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * Everything needed to draw one plan: projected tract polygons, the assignment, per-district D
 * shares and the unioned district polygons, plus title lines.
 */
public record MapData(String title, String subtitle, List<Geometry> tracts, int[] assignment, int districts, double[] demShare, Geometry[] districtShapes) {
    
    public static MapData of(String title, String subtitle, List<Geometry> tracts, int[] assignment, int districts, double[] demShare) {
        return new MapData(title, subtitle, tracts, assignment, districts, demShare, DistrictShapes.of(tracts, assignment, districts));
    }
    
    public Envelope bounds() {
        Envelope e = new Envelope();
        for (Geometry g : districtShapes) {
            e.expandToInclude(g.getEnvelopeInternal());
        }
        return e;
    }
}
