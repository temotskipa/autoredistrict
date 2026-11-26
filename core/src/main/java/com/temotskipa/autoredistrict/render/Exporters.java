package com.temotskipa.autoredistrict.render;

import com.temotskipa.autoredistrict.data.Shapefile;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * District exports: GeoJSON (WGS84-style lon/lat), ESRI shapefile (projected), assignment CSV.
 */
@SuppressWarnings("preview") // LazyConstant is a preview API, used on purpose
public final class Exporters {
    
    private static final List<Shapefile.Field> SHAPEFILE_FIELDS = List.of(Shapefile.Field.number("DISTRICT", 9, 0), Shapefile.Field.number("POP", 19, 0), Shapefile.Field.number("DEV_PCT", 12, 4), Shapefile.Field.number("D_SHARE", 10, 5), Shapefile.Field.text("WINNER", 1), Shapefile.Field.number("MARGIN", 10, 3));
    
    private static final LazyConstant<JsonMapper> JSON = LazyConstant.of(() -> JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).build());
    
    private Exporters() {
    }
    
    /**
     * Writes district polygons as a GeoJSON FeatureCollection. {@code toLonLat} inverse-projects
     * projected geometries (the state's Albers) to longitude/latitude.
     */
    public static void geoJson(Path file, Geometry[] districts, List<DistrictProps> props, Function<Geometry, Geometry> toLonLat) {
        ObjectNode root = JSON.get().createObjectNode();
        root.put("type", "FeatureCollection");
        ArrayNode features = root.putArray("features");
        for (int d = 0; d < districts.length; d++) {
            ObjectNode f = features.addObject();
            f.put("type", "Feature");
            DistrictProps p = props.get(d);
            ObjectNode pr = f.putObject("properties");
            pr.put("district", p.district());
            pr.put("population", p.population());
            pr.put("deviation_pct", round(p.deviationPct(), 4));
            pr.put("dem_share", round(p.demShare(), 5));
            pr.put("winner", p.winner());
            pr.put("margin_pts", round(p.marginPts(), 3));
            f.set("geometry", geometry(toLonLat.apply(districts[d])));
        }
        try (OutputStream out = Files.newOutputStream(file)) {
            JSON.get().writeValue(out, root);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write " + file, e);
        }
    }
    
    private static ObjectNode geometry(Geometry g) {
        ObjectNode node = JSON.get().createObjectNode();
        node.put("type", "MultiPolygon");
        ArrayNode polys = node.putArray("coordinates");
        for (int i = 0; i < g.getNumGeometries(); i++) {
            if (!(g.getGeometryN(i) instanceof Polygon p)) {
                continue;
            }
            ArrayNode rings = polys.addArray();
            ring(rings.addArray(), p.getExteriorRing());
            for (int h = 0; h < p.getNumInteriorRing(); h++) {
                ring(rings.addArray(), p.getInteriorRingN(h));
            }
        }
        return node;
    }
    
    private static void ring(ArrayNode arr, LineString ring) {
        for (Coordinate c : ring.getCoordinates()) {
            ArrayNode pt = arr.addArray();
            pt.add(round(c.x, 6));
            pt.add(round(c.y, 6));
        }
    }
    
    private static double round(double v, int digits) {
        double f = Math.pow(10, digits);
        return Math.round(v * f) / f;
    }
    
    /**
     * Writes district polygons as a shapefile in the projected CRS given as WKT.
     */
    public static void shapefile(Path shp, Geometry[] districts, List<DistrictProps> props, String projectedWkt) {
        List<Shapefile.Feature> features = new ArrayList<>(districts.length);
        for (int d = 0; d < districts.length; d++) {
            DistrictProps p = props.get(d);
            features.add(new Shapefile.Feature(districts[d], List.of(p.district(), p.population(), round(p.deviationPct(), 4), round(p.demShare(), 5), p.winner(), round(p.marginPts(), 3))));
        }
        try {
            Shapefile.write(shp, projectedWkt, SHAPEFILE_FIELDS, features);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write " + shp, e);
        }
    }
    
    /**
     * GEOID,district (1-based) per tract.
     */
    public static void assignmentCsv(Path file, String[] geoids, int[] assignment) {
        var sb = new StringBuilder("GEOID20,district\n");
        for (int i = 0; i < geoids.length; i++) {
            sb.append(geoids[i]).append(',').append(assignment[i] + 1).append('\n');
        }
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
            Files.writeString(file, sb, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write " + file, e);
        }
    }
    
    /**
     * Properties of one district for the exports.
     */
    public value record DistrictProps(int district, long population, double deviationPct, double demShare, String winner, double marginPts) {
    }
}
