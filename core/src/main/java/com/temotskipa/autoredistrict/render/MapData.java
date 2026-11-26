package com.temotskipa.autoredistrict.render;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.Arrays;
import java.util.List;

/**
 * Everything needed to draw one plan: projected tract polygons, the assignment, per-district D
 * shares and the district polygons, plus title lines. {@code districtShapes} are the whole
 * districts, for the exports; {@code drawnShapes} leave out hidden tracts, for the map.
 * {@code focus} (may be null) is the extent to frame, for example the populated tracts, so that
 * remote uninhabited tracts (Hawaii's northwestern islands) don't shrink the map. {@code shades}
 * (may be null: all {@link Shade#FULL}) says how each tract is drawn.
 */
public value record MapData(String title, String subtitle, List<Geometry> tracts, int[] assignment, int districts, double[] demShare, Geometry[] districtShapes, Geometry[] drawnShapes, Envelope focus, Shade[] shades) {
    
    public static MapData of(String title, String subtitle, List<Geometry> tracts, int[] assignment, int districts, double[] demShare) {
        return of(title, subtitle, tracts, assignment, districts, demShare, null, null);
    }
    
    public static MapData of(String title, String subtitle, List<Geometry> tracts, int[] assignment, int districts, double[] demShare, Envelope focus, Shade[] shades) {
        Geometry[] whole = DistrictShapes.of(tracts, assignment, districts);
        Geometry[] drawn = whole;
        if (shades != null && Arrays.asList(shades).contains(Shade.HIDDEN)) {
            drawn = DistrictShapes.of(tracts, assignment, districts, i -> shades[i] != Shade.HIDDEN);
        }
        return new MapData(title, subtitle, tracts, assignment, districts, demShare, whole, drawn, focus, shades);
    }
    
    /**
     * The extent to frame: the focus if given, else the drawn districts.
     */
    public Envelope bounds() {
        if (focus != null && !focus.isNull()) {
            return focus;
        }
        var e = new Envelope();
        for (Geometry g : drawnShapes) {
            e.expandToInclude(g.getEnvelopeInternal());
        }
        return e;
    }
    
    public Shade shade(int tract) {
        return shades == null ? Shade.FULL : shades[tract];
    }
    
    /**
     * How a tract is drawn.
     */
    public enum Shade {
        /**
         * In its district's colour.
         */
        FULL,
        /**
         * Faded toward the background: a tract without people.
         */
        FADED,
        /**
         * Not drawn, and left out of the drawn district outlines: a water-only tract, so maps
         * follow the coastline.
         */
        HIDDEN
    }
}
