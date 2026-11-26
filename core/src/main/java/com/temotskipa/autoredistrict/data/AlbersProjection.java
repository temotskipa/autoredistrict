package com.temotskipa.autoredistrict.data;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.geom.util.PolygonExtracter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.*;

/**
 * A per-state Albers equal-area conic projection with standard parallels at 1/6 and 5/6 of the
 * state's latitude range, centred on its bounding box. It works on the ellipsoid of the source
 * file's own geographic coordinate system (GRS 1980, from NAD83, for RDH), so no datum shift
 * happens and no EPSG database is needed.
 *
 * <p>The formulas are Snyder's (1987, eqs. 3-12, 3-16 and 14-1 to 14-21), evaluated in the same
 * order and with the same tolerances as GeoTools' {@code AlbersEqualArea}, which earlier versions
 * of the importer used, so caches they wrote project to the same coordinates. The parameters are
 * the ones the WKT states, to 10 decimals, so a {@code .prj} written from {@link #wkt()} describes
 * exactly this projection.
 *
 * <p>Alaska's Aleutians cross the antimeridian. Longitudes are unwrapped to within 180 degrees of
 * the central meridian before projecting, so they land west of the rest of the state.
 */
public value class AlbersProjection {
    
    /**
     * NAD83 as RDH's {@code .prj} files spell it.
     */
    public static final String NAD83_ESRI = "GEOGCS[\"GCS_North_American_1983\",DATUM[\"D_North_American_1983\"," + "SPHEROID[\"GRS_1980\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0]," + "UNIT[\"Degree\",0.0174532925199433]]";
    
    private static final double EPSILON = 1e-6;
    private static final double ITERATION_TOLERANCE = 1e-10;
    private static final int MAX_ITERATIONS = 15;
    private static final double FALSE_EASTING = 0.0;
    private static final double FALSE_NORTHING = 0.0;
    
    private static final String NUMBER = "([-+]?(?:\\d+\\.?\\d*|\\.\\d+)(?:[eE][-+]?\\d+)?)";
    private static final Pattern SPHEROID = Pattern.compile("SPHEROID\\s*\\[\\s*\"[^\"]*\"\\s*,\\s*" + NUMBER + "\\s*,\\s*" + NUMBER, Pattern.CASE_INSENSITIVE);
    private static final Pattern PRIME_MERIDIAN = Pattern.compile("PRIMEM\\s*\\[\\s*\"[^\"]*\"\\s*,\\s*" + NUMBER, Pattern.CASE_INSENSITIVE);
    private static final Pattern UNIT = Pattern.compile("UNIT\\s*\\[\\s*\"[^\"]*\"\\s*,\\s*" + NUMBER, Pattern.CASE_INSENSITIVE);
    
    private final double lat1;
    private final double lat2;
    private final double lat0;
    private final double lon0;
    private final String geographicWkt;
    private final String wkt;
    
    // Projection constants, in radians and units of the semi-major axis, named as in GeoTools.
    private final double semiMajor;
    private final double excentricitySquared;
    private final double excentricity;
    private final double centralMeridian;
    private final double n;
    private final double c;
    private final double rho0;
    private final double ec;
    
    private AlbersProjection(double lat1, double lat2, double lat0, double lon0, String geographicWkt) {
        this.lat1 = lat1;
        this.lat2 = lat2;
        this.lat0 = lat0;
        this.lon0 = lon0;
        this.geographicWkt = geographicWkt;
        this.wkt = String.format(Locale.ROOT, "PROJCS[\"Albers equal-area (autoredistrict)\",%s,PROJECTION[\"Albers_Conic_Equal_Area\"]," + "PARAMETER[\"standard_parallel_1\",%.10f],PARAMETER[\"standard_parallel_2\",%.10f]," + "PARAMETER[\"latitude_of_center\",%.10f],PARAMETER[\"longitude_of_center\",%.10f]," + "PARAMETER[\"false_easting\",0.0],PARAMETER[\"false_northing\",0.0],UNIT[\"metre\",1.0]]", geographicWkt, lat1, lat2, lat0, lon0);
        
        double[] ellipsoid = ellipsoid(geographicWkt);
        double a = ellipsoid[0];
        double b = a * (1 - 1 / ellipsoid[1]);
        double es = 1.0 - b * b / (a * a);
        double e = sqrt(es);
        double phi1 = toRadians(asWritten(lat1));
        double phi2 = toRadians(asWritten(lat2));
        if (abs(phi1 + phi2) < EPSILON) {
            throw new DataException("Could not build the Albers projection: its standard parallels are opposite latitudes.");
        }
        double sinphi = sin(phi1);
        double cosphi = cos(phi1);
        double cone = sinphi;
        double m1 = msfn(sinphi, cosphi, es);
        double q1 = qsfn(sinphi, e, es);
        if (abs(phi1 - phi2) >= EPSILON) {
            sinphi = sin(phi2);
            cosphi = cos(phi2);
            double m2 = msfn(sinphi, cosphi, es);
            double q2 = qsfn(sinphi, e, es);
            cone = (m1 * m1 - m2 * m2) / (q2 - q1);
        }
        double constant = m1 * m1 + cone * q1;
        this.semiMajor = a;
        this.excentricitySquared = es;
        this.excentricity = e;
        this.centralMeridian = toRadians(asWritten(lon0));
        this.n = cone;
        this.c = constant;
        this.rho0 = sqrt(constant - cone * qsfn(sin(toRadians(asWritten(lat0))), e, es)) / cone;
        this.ec = 1.0 - .5 * (1.0 - es) * log((1.0 - e) / (1.0 + e)) / e;
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
        return transformed(lonLat, (lon, lat, out) -> forward(unwrap(lon), lat, out));
    }
    
    /**
     * Projects one lon/lat point to metres.
     */
    public Coordinate project(double lon, double lat) {
        var p = new double[2];
        forward(unwrap(lon), lat, p);
        return new Coordinate(p[0], p[1]);
    }
    
    /**
     * Inverse-projects metres to lon/lat, with longitudes wrapped back into [-180, 180].
     */
    public Coordinate toLonLat(double x, double y) {
        var p = new double[2];
        inverse(x, y, p);
        return new Coordinate(wrap(p[0]), p[1]);
    }
    
    /**
     * Inverse-projects a geometry in metres to lon/lat. Longitudes stay continuous around the
     * central meridian while projecting. A polygonal geometry that then crosses the antimeridian
     * (Alaska's Aleutians) is cut there, as RFC 7946 asks of GeoJSON, so no ring spans the globe;
     * other geometries get their longitudes wrapped into [-180, 180].
     */
    public Geometry toLonLat(Geometry projected) {
        Geometry copy = transformed(projected, (x, y, out) -> {
            inverse(x, y, out);
            out[0] = unwrap(out[0]);
        });
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
    
    /**
     * Snyder 14-1 to 14-6: degrees to metres.
     */
    private void forward(double lon, double lat, double[] out) {
        double x = rollLongitude(toRadians(lon) - centralMeridian);
        double y = toRadians(lat);
        x *= n;
        double rho = c - n * qsfn(sin(y), excentricity, excentricitySquared);
        if (rho < 0.0) {
            if (rho > -EPSILON) {
                rho = 0.0;
            }
            else {
                throw new DataException(String.format(Locale.ROOT, "Could not project the point %.6f, %.6f: it is outside the Albers projection.", lon, lat));
            }
        }
        rho = sqrt(rho) / n;
        y = rho0 - rho * cos(x);
        x = rho * sin(x);
        out[0] = semiMajor * x + FALSE_EASTING;
        out[1] = semiMajor * y + FALSE_NORTHING;
    }
    
    /**
     * Snyder 14-8 to 14-11 and 3-16: metres to degrees, longitudes in [-180, 180].
     */
    private void inverse(double x0, double y0, double[] out) {
        double x = (x0 - FALSE_EASTING) / semiMajor;
        double y = rho0 - (y0 - FALSE_NORTHING) / semiMajor;
        double rho = hypot(x, y);
        double lambda;
        double phi;
        if (rho > EPSILON) {
            if (n < 0.0) {
                rho = -rho;
                x = -x;
                y = -y;
            }
            lambda = atan2(x, y) / n;
            double t = rho * n;
            double q = (c - t * t) / n;
            phi = abs(ec - abs(q)) > EPSILON ? phi1(q, excentricity, excentricitySquared) : (q < 0.0 ? -PI / 2.0 : PI / 2.0);
        }
        else {
            lambda = 0.0;
            phi = n > 0.0 ? PI / 2.0 : -PI / 2.0;
        }
        out[0] = toDegrees(rollLongitude(lambda + centralMeridian));
        out[1] = toDegrees(phi);
    }
    
    /**
     * Snyder 3-12: q for the sine of a latitude.
     */
    private static double qsfn(double sinphi, double e, double es) {
        double oneEs = 1 - es;
        double con = e * sinphi;
        return oneEs * (sinphi / (1. - con * con) - 0.5 / e * log((1. - con) / (1. + con)));
    }
    
    /**
     * Snyder 14-15: m for the sine and cosine of a latitude.
     */
    private static double msfn(double s, double c, double es) {
        return c / sqrt(1.0 - s * s * es);
    }
    
    /**
     * Snyder 3-16: the latitude for q, by iteration.
     */
    private static double phi1(double qs, double e, double es) {
        double toneEs = 1 - es;
        double phi = asin(0.5 * qs);
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            double sinpi = sin(phi);
            double cospi = cos(phi);
            double con = e * sinpi;
            double com = 1.0 - con * con;
            double dphi = 0.5 * com * com / cospi * (qs / toneEs - sinpi / com + 0.5 / e * log((1. - con) / (1. + con)));
            phi += dphi;
            if (abs(dphi) <= ITERATION_TOLERANCE) {
                return phi;
            }
        }
        throw new DataException("Could not inverse-project a point: the latitude did not converge.");
    }
    
    /**
     * An angle in radians moved into [-pi, pi).
     */
    private static double rollLongitude(double x) {
        return x - 2 * PI * floor(x / (2 * PI) + 0.5);
    }
    
    /**
     * The value as the WKT writes it, with 10 decimals.
     */
    private static double asWritten(double degrees) {
        return Double.parseDouble(String.format(Locale.ROOT, "%.10f", degrees));
    }
    
    /**
     * The semi-major axis and inverse flattening of a geographic WKT's ellipsoid, after checking it
     * is longitude/latitude in degrees from Greenwich.
     */
    private static double[] ellipsoid(String geographicWkt) {
        String problem = null;
        Matcher spheroid = SPHEROID.matcher(geographicWkt);
        Matcher meridian = PRIME_MERIDIAN.matcher(geographicWkt);
        Matcher unit = UNIT.matcher(geographicWkt);
        if (!geographicWkt.strip().regionMatches(true, 0, "GEOGCS", 0, 6)) {
            problem = "the coordinate system isn't longitude/latitude (GEOGCS)";
        }
        else if (!spheroid.find()) {
            problem = "the coordinate system names no SPHEROID";
        }
        else if (meridian.find() && Double.parseDouble(meridian.group(1)) != 0.0) {
            problem = "the prime meridian isn't Greenwich";
        }
        else if (unit.find() && abs(Double.parseDouble(unit.group(1)) / toRadians(1) - 1) > 1e-9) {
            problem = "the angular unit isn't degrees";
        }
        else if (!(Double.parseDouble(spheroid.group(2)) > 1)) {
            problem = "the ellipsoid is a sphere (inverse flattening " + spheroid.group(2) + ")";
        }
        if (problem != null) {
            throw new DataException("Could not build the Albers projection: " + problem + ". RDH tract files are NAD83 longitude/latitude.");
        }
        return new double[]{Double.parseDouble(spheroid.group(1)), Double.parseDouble(spheroid.group(2))};
    }
    
    /**
     * A copy of {@code g} with {@code op} applied to every coordinate.
     */
    private static Geometry transformed(Geometry g, PointTransform op) {
        Geometry copy = g.copy();
        var p = new double[2];
        copy.apply(new CoordinateSequenceFilter() {
            @Override
            public void filter(CoordinateSequence seq, int i) {
                op.apply(seq.getX(i), seq.getY(i), p);
                seq.setOrdinate(i, CoordinateSequence.X, p[0]);
                seq.setOrdinate(i, CoordinateSequence.Y, p[1]);
            }
            
            @Override
            public boolean isDone() {
                return false;
            }
            
            @Override
            public boolean isGeometryChanged() {
                return true;
            }
        });
        return copy;
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
    
    @FunctionalInterface
    private interface PointTransform {
        void apply(double x, double y, double[] out);
    }
}
