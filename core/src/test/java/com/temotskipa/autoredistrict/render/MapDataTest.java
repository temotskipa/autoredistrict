package com.temotskipa.autoredistrict.render;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MapDataTest {
    
    private static final GeometryFactory GF = new GeometryFactory();
    
    private static Geometry square(double x) {
        return GF.createPolygon(new Coordinate[]{new Coordinate(x, 0), new Coordinate(x + 1, 0), new Coordinate(x + 1, 1), new Coordinate(x, 1), new Coordinate(x, 0)});
    }
    
    @Test
    void hiddenWaterTractsStayInTheDistrictButOutOfTheDrawnMap() {
        // Two land tracts and a coastal water tract east of them, all in one district.
        List<Geometry> tracts = List.of(square(0), square(1), square(2));
        var shades = new MapData.Shade[]{MapData.Shade.FULL, MapData.Shade.FADED, MapData.Shade.HIDDEN};
        MapData data = MapData.of("t", "s", tracts, new int[]{0, 0, 0}, 1, new double[]{0.4}, null, shades);
        
        assertEquals(3.0, data.districtShapes()[0].getArea(), 1e-9, "exports keep the whole district");
        assertEquals(2.0, data.drawnShapes()[0].getArea(), 1e-9, "the map leaves the water out");
        assertEquals(new Envelope(0, 2, 0, 1), data.bounds());
    }
    
    @Test
    void withoutShadesEveryTractIsDrawn() {
        MapData data = MapData.of("t", "s", List.of(square(0), square(1)), new int[]{0, 1}, 2, new double[]{0.6, 0.4});
        assertEquals(MapData.Shade.FULL, data.shade(1));
        assertEquals(1.0, data.drawnShapes()[1].getArea(), 1e-9);
    }
}
