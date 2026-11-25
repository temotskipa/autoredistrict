package com.temotskipa.autoredistrict.render;

import org.geotools.api.data.FeatureWriter;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.referencing.FactoryException;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.*;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * District exports: GeoJSON (WGS84-style lon/lat), ESRI shapefile (projected), assignment CSV.
 */
@SuppressWarnings("preview") // LazyConstant is a preview API, used on purpose
public final class Exporters {

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
        try {
            Files.createDirectories(shp.toAbsolutePath().getParent());
            var b = new SimpleFeatureTypeBuilder();
            var name = shp.getFileName().toString().replaceFirst("\\.shp$", "");
            b.setName(name);
            b.setCRS(CRS.parseWKT(projectedWkt));
            b.add("the_geom", MultiPolygon.class);
            b.add("DISTRICT", Integer.class);
            b.add("POP", Long.class);
            b.add("DEV_PCT", Double.class);
            b.add("D_SHARE", Double.class);
            b.length(1).add("WINNER", String.class);
            b.add("MARGIN", Double.class);
            var type = b.buildFeatureType();
            Map<String, Serializable> params = new HashMap<>();
            params.put("url", shp.toUri().toURL());
            params.put("create spatial index", Boolean.FALSE);
            ShapefileDataStore ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);
            try {
                ds.setCharset(StandardCharsets.UTF_8);
                ds.createSchema(type);
                try (FeatureWriter<SimpleFeatureType, SimpleFeature> w = ds.getFeatureWriterAppend(Transaction.AUTO_COMMIT)) {
                    for (int d = 0; d < districts.length; d++) {
                        DistrictProps p = props.get(d);
                        SimpleFeature f = w.next();
                        Geometry g = districts[d];
                        if (g instanceof Polygon poly) {
                            g = g.getFactory().createMultiPolygon(new Polygon[]{poly});
                        }
                        f.setAttribute("the_geom", g);
                        f.setAttribute("DISTRICT", p.district());
                        f.setAttribute("POP", p.population());
                        f.setAttribute("DEV_PCT", round(p.deviationPct(), 4));
                        f.setAttribute("D_SHARE", round(p.demShare(), 5));
                        f.setAttribute("WINNER", p.winner());
                        f.setAttribute("MARGIN", round(p.marginPts(), 3));
                        w.write();
                    }
                }
            } finally {
                ds.dispose();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write " + shp, e);
        } catch (FactoryException e) {
            throw new IllegalStateException("Bad projection WKT: " + e.getMessage(), e);
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
    public record DistrictProps(int district, long population, double deviationPct, double demShare, String winner, double marginPts) {
    }
}
