package com.temotskipa.autoredistrict.data;

import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.api.referencing.operation.NoninvertibleTransformException;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.geom.util.PolygonExtracter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A per-state Albers equal-area conic projection with standard parallels at 1/6 and 5/6 of the
 * state's latitude range, centred on its bounding box. It is defined with GeoTools from WKT
 * parameters on the source file's own geographic CRS (NAD83 for RDH), so no EPSG database is
 * needed and no datum shift happens.
 *
 * <p>Alaska's Aleutians cross the antimeridian. Longitudes are unwrapped to within 180 degrees of
 * the central meridian before projecting, so they land west of the rest of the state.
 */
public value class AlbersProjection {
    
    /**
     * NAD83 as RDH's {@code .prj} files spell it.
     */
    public static final String NAD83_ESRI = "GEOGCS[\"GCS_North_American_1983\",DATUM[\"D_North_American_1983\"," + "SPHEROID[\"GRS_1980\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0]," + "UNIT[\"Degree\",0.0174532925199433]]";
    
    /**
     * GeoTools logs a warning for longitudes beyond -180, which unwrapped Aleutian coordinates are.
     * The projection handles them correctly (see GeometryTest), so that logger is quietened. The
     * field keeps a strong reference, since java.util.logging holds loggers weakly.
     */
    private static final Logger PROJECTION_LOG;
    
    static {
        PROJECTION_LOG = Logger.getLogger("org.geotools.referencing.operation.projection");
        PROJECTION_LOG.setLevel(Level.SEVERE);
    }
    
    private final double lat1;
    private final double lat2;
    private final double lat0;
    private final double lon0;
    private final String geographicWkt;
    private final String wkt;
    private final MathTransform forward;
    private final MathTransform inverse;
    
    private AlbersProjection(double lat1, double lat2, double lat0, double lon0, String geographicWkt) {
        this.lat1 = lat1;
        this.lat2 = lat2;
        this.lat0 = lat0;
        this.lon0 = lon0;
        this.geographicWkt = geographicWkt;
        this.wkt = String.format(Locale.ROOT, "PROJCS[\"Albers equal-area (autoredistrict)\",%s,PROJECTION[\"Albers_Conic_Equal_Area\"]," + "PARAMETER[\"standard_parallel_1\",%.10f],PARAMETER[\"standard_parallel_2\",%.10f]," + "PARAMETER[\"latitude_of_center\",%.10f],PARAMETER[\"longitude_of_center\",%.10f]," + "PARAMETER[\"false_easting\",0.0],PARAMETER[\"false_northing\",0.0],UNIT[\"metre\",1.0]]", geographicWkt, lat1, lat2, lat0, lon0);
        try {
            CoordinateReferenceSystem source = CRS.parseWKT(geographicWkt);
            CoordinateReferenceSystem target = CRS.parseWKT(wkt);
            this.forward = CRS.findMathTransform(source, target, true);
            this.inverse = forward.inverse();
        } catch (FactoryException | NoninvertibleTransformException e) {
            throw new DataException("Could not build the Albers projection: " + e.getMessage(), e);
        }
    }
    
    /**
     * Builds the projection from explicit parameters (as stored in the cache manifest).
     */
    public static AlbersProjection of(double lat1, double lat2, double lat0, double lon0, String geographicWkt) {
        return new AlbersProjection(lat1, lat2, lat0, lon0, geographicWkt);
    }
    
    /**
     * Fits a projection to a lon/lat bounding box whose longitudes are already unwrapped (see
     * {@link #unwrappedBounds(Iterable)}).
     */
    public static AlbersProjection forBounds(Envelope lonLat, String geographicWkt) {
        double minLat = lonLat.getMinY();
        double maxLat = lonLat.getMaxY();
        double range = maxLat - minLat;
        return new AlbersProjection(minLat + range / 6.0, minLat + 5.0 * range / 6.0, (minLat + maxLat) / 2.0, (lonLat.getMinX() + lonLat.getMaxX()) / 2.0, geographicWkt);
    }
    
    /**
     * The lon/lat bounds of the geometries. When they straddle the antimeridian (some longitudes
     * above +170 and some below -170), eastern-hemisphere longitudes are shifted by -360.
     */
    public static Envelope unwrappedBounds(Iterable<Geometry> geometries) {
        var raw = new Envelope();
        for (Geometry g : geometries) {
            if (g != null && !g.isEmpty()) {
                raw.expandToInclude(g.getEnvelopeInternal());
            }
        }
        if (raw.isNull() || !(raw.getMinX() < -170 && raw.getMaxX() > 170)) {
            return raw;
        }
        var shifted = new Envelope();
        for (Geometry g : geometries) {
            if (g == null) {
                continue;
            }
            for (Coordinate c : g.getCoordinates()) {
                shifted.expandToInclude(c.x > 0 ? c.x - 360 : c.x, c.y);
            }
        }
        return shifted;
    }
    
    /**
     * Projects a lon/lat geometry to metres. The input isn't modified.
     */
    public Geometry project(Geometry lonLat) {
        Geometry copy = lonLat.copy();
        copy.apply((CoordinateFilter) c -> c.x = unwrap(c.x));
        copy.geometryChanged();
        try {
            return JTS.transform(copy, forward);
        } catch (TransformException e) {
            throw new DataException("Could not project a geometry: " + e.getMessage(), e);
        }
    }
    
    /**
     * Projects one lon/lat point to metres.
     */
    public Coordinate project(double lon, double lat) {
        double[] p = {unwrap(lon), lat};
        try {
            forward.transform(p, 0, p, 0, 1);
        } catch (TransformException e) {
            throw new DataException("Could not project a point: " + e.getMessage(), e);
        }
        return new Coordinate(p[0], p[1]);
    }
    
    /**
     * Inverse-projects metres to lon/lat, with longitudes wrapped back into [-180, 180].
     */
    public Coordinate toLonLat(double x, double y) {
        double[] p = inverse(x, y);
        return new Coordinate(wrap(p[0]), p[1]);
    }
    
    /**
     * Inverse-projects a geometry in metres to lon/lat. Longitudes stay continuous around the
     * central meridian while projecting. A polygonal geometry that then crosses the antimeridian
     * (Alaska's Aleutians) is cut there, as RFC 7946 asks of GeoJSON, so no ring spans the globe;
     * other geometries get their longitudes wrapped into [-180, 180].
     */
    public Geometry toLonLat(Geometry projected) {
        Geometry copy = projected.copy();
        copy.apply((CoordinateFilter) c -> {
            double[] p = inverse(c.x, c.y);
            c.x = unwrap(p[0]);
            c.y = p[1];
        });
        copy.geometryChanged();
        Envelope env = copy.getEnvelopeInternal();
        if (env.isNull() || (env.getMinX() >= -180 && env.getMaxX() <= 180)) {
            return copy;
        }
        if (copy instanceof Polygonal) {
            return cutAtAntimeridian(copy);
        }
        copy.apply((CoordinateFilter) c -> c.x = wrap(c.x));
        copy.geometryChanged();
        return copy;
    }
    
    /**
     * Splits a polygonal lon/lat geometry with continuous longitudes into the parts east and west of
     * the antimeridian, each shifted by 360 degrees as needed to lie within [-180, 180].
     */
    static Geometry cutAtAntimeridian(Geometry g) {
        GeometryFactory f = g.getFactory();
        List<Polygon> parts = new ArrayList<>();
        for (int shift = -360; shift <= 360; shift += 360) {
            // The slice that lands in [-180, 180] once moved by `shift` degrees.
            Geometry slice = g.intersection(f.toGeometry(new Envelope(-180 - shift, 180 - shift, -90, 90)));
            if (slice.isEmpty()) {
                continue;
            }
            if (shift != 0) {
                slice = AffineTransformation.translationInstance(shift, 0).transform(slice);
            }
            for (Object p : PolygonExtracter.getPolygons(slice)) {
                parts.add((Polygon) p);
            }
        }
        return f.createMultiPolygon(parts.toArray(Polygon[]::new));
    }
    
    private double[] inverse(double x, double y) {
        double[] p = {x, y};
        try {
            inverse.transform(p, 0, p, 0, 1);
        } catch (TransformException e) {
            throw new DataException("Could not inverse-project a point: " + e.getMessage(), e);
        }
        return p;
    }
    
    private static double wrap(double lon) {
        while (lon < -180) {
            lon += 360;
        }
        while (lon > 180) {
            lon -= 360;
        }
        return lon;
    }
    
    private double unwrap(double lon) {
        double d = lon - lon0;
        if (d > 180) {
            return lon - 360;
        }
        if (d < -180) {
            return lon + 360;
        }
        return lon;
    }
    
    public double standardParallel1() {
        return lat1;
    }
    
    public double standardParallel2() {
        return lat2;
    }
    
    public double latitudeOfCenter() {
        return lat0;
    }
    
    public double longitudeOfCenter() {
        return lon0;
    }
    
    public String geographicWkt() {
        return geographicWkt;
    }
    
    /**
     * The projected CRS as WKT, for {@code .prj} files.
     */
    public String wkt() {
        return wkt;
    }
}
