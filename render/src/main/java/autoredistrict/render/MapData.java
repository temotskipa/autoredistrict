package autoredistrict.render;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * Everything needed to draw one plan: projected tract polygons, the assignment, per-district D
 * shares and the unioned district polygons, plus title lines. {@code focus} (may be null) is the
 * extent to frame, for example the populated tracts, so that remote uninhabited tracts (Hawaii's
 * northwestern islands) don't shrink the map. {@code empty} (may be null) marks tracts without
 * people (mostly water), which are drawn in a pale tint.
 */
public record MapData(String title, String subtitle, List<Geometry> tracts, int[] assignment, int districts, double[] demShare, Geometry[] districtShapes, Envelope focus, boolean[] empty) {
    
    public static MapData of(String title, String subtitle, List<Geometry> tracts, int[] assignment, int districts, double[] demShare) {
        return of(title, subtitle, tracts, assignment, districts, demShare, null, null);
    }
    
    public static MapData of(String title, String subtitle, List<Geometry> tracts, int[] assignment, int districts, double[] demShare, Envelope focus, boolean[] empty) {
        return new MapData(title, subtitle, tracts, assignment, districts, demShare, DistrictShapes.of(tracts, assignment, districts), focus, empty);
    }
    
    /**
     * The extent to frame: the focus if given, else all districts.
     */
    public Envelope bounds() {
        if (focus != null && !focus.isNull()) {
            return focus;
        }
        Envelope e = new Envelope();
        for (Geometry g : districtShapes) {
            e.expandToInclude(g.getEnvelopeInternal());
        }
        return e;
    }
    
    public boolean isEmpty(int tract) {
        return empty != null && empty[tract];
    }
}
