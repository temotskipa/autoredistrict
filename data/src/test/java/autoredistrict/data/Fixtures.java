package autoredistrict.data;

import org.geotools.api.data.FeatureWriter;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Small synthetic RDH-shaped fixtures: a grid of square tracts with legacy PL columns
 * (GEOID20, NAME, P0010001), written as a shapefile and zipped like {@code xx_pl2020_t.zip}.
 */
final class Fixtures {
    
    static final GeometryFactory GF = new GeometryFactory();
    
    private Fixtures() {
    }
    
    /**
     * A size x size grid of 0.1-degree tracts starting at (-70, 44), county 001.
     */
    static List<Tract> grid(String state, int size) {
        List<Tract> out = new ArrayList<>();
        for (int row = 0; row < size; row++) {
            for (int col = 0; col < size; col++) {
                int index = row * size + col;
                out.add(new Tract(String.format("%s001%04d00", state, index + 1), "Census Tract " + (index + 1), 1000 + 10L * index, box(-70 + col * 0.1, 44 + row * 0.1, -70 + (col + 1) * 0.1, 44 + (row + 1) * 0.1)));
            }
        }
        return out;
    }
    
    static Polygon box(double x0, double y0, double x1, double y1) {
        return GF.createPolygon(new Coordinate[]{new Coordinate(x0, y0), new Coordinate(x1, y0), new Coordinate(x1, y1), new Coordinate(x0, y1), new Coordinate(x0, y0)});
    }
    
    static Polygon poly(double... xy) {
        Coordinate[] c = new Coordinate[xy.length / 2 + 1];
        for (int i = 0; i < xy.length / 2; i++) {
            c[i] = new Coordinate(xy[2 * i], xy[2 * i + 1]);
        }
        c[c.length - 1] = c[0].copy();
        return GF.createPolygon(c);
    }
    
    /**
     * Writes a shapefile with GEOID20, NAME and (optionally) P0010001 into {@code folder/name.shp}.
     */
    static Path shapefile(Path folder, String name, List<Tract> tracts, boolean withCounts) throws Exception {
        Files.createDirectories(folder);
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.setName(name);
        b.setCRS(CRS.parseWKT(AlbersProjection.NAD83_ESRI));
        b.add("the_geom", MultiPolygon.class);
        b.length(15).add("GEOID20", String.class);
        b.length(40).add("NAME", String.class);
        if (withCounts) {
            b.add("P0010001", Long.class);
        }
        SimpleFeatureType type = b.buildFeatureType();
        Path shp = folder.resolve(name + ".shp");
        Map<String, Serializable> params = new HashMap<>();
        params.put("url", shp.toUri().toURL());
        params.put("create spatial index", Boolean.FALSE);
        ShapefileDataStore ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);
        try {
            ds.createSchema(type);
            try (FeatureWriter<SimpleFeatureType, SimpleFeature> w = ds.getFeatureWriterAppend(Transaction.AUTO_COMMIT)) {
                for (Tract t : tracts) {
                    SimpleFeature f = w.next();
                    Geometry g = t.geometry() instanceof Polygon p ? GF.createMultiPolygon(new Polygon[]{p}) : t.geometry();
                    f.setAttribute("the_geom", g);
                    f.setAttribute("GEOID20", t.geoid());
                    f.setAttribute("NAME", t.name());
                    if (withCounts) {
                        f.setAttribute("P0010001", t.pop());
                    }
                    w.write();
                }
            }
        } finally {
            ds.dispose();
        }
        return shp;
    }
    
    /**
     * Zips every file in {@code folder} (flat) into {@code zip}, adding an RDH-style readme.
     */
    static Path zipFolder(Path folder, Path zip) throws IOException {
        Files.writeString(folder.resolve("README.txt"), "RDH readme");
        try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zip)); var files = Files.list(folder)) {
            for (Path f : files.sorted().toList()) {
                out.putNextEntry(new ZipEntry(f.getFileName().toString()));
                Files.copy(f, out);
                out.closeEntry();
            }
        }
        return zip;
    }
    
    /**
     * An RDH tract zip ({@code name.zip}) for the given tracts.
     */
    static Path tractZip(Path tmp, String name, List<Tract> tracts) throws Exception {
        Path folder = tmp.resolve("download-" + name);
        shapefile(folder, name, tracts, true);
        return zipFolder(folder, tmp.resolve(name + ".zip"));
    }
    
    /**
     * Zips a test resource CSV as the single CSV member of {@code zip}, like RDH's vote zips.
     */
    static Path zipResource(String resource, Path zip, String member) throws IOException {
        try (InputStream in = Fixtures.class.getResourceAsStream(resource);
             ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zip))) {
            if (in == null) {
                throw new IOException("missing test resource " + resource);
            }
            out.putNextEntry(new ZipEntry("README.txt"));
            out.write("readme".getBytes());
            out.closeEntry();
            out.putNextEntry(new ZipEntry(member));
            in.transferTo(out);
            out.closeEntry();
        }
        return zip;
    }
    
    static InputStream resource(String name) {
        InputStream in = Fixtures.class.getResourceAsStream(name);
        if (in == null) {
            throw new IllegalArgumentException("missing test resource " + name);
        }
        return in;
    }
    
    static void write(Path file, String text) throws IOException {
        Files.createDirectories(file.getParent());
        try (OutputStream out = Files.newOutputStream(file)) {
            out.write(text.getBytes());
        }
    }
    
    record Tract(String geoid, String name, long pop, Geometry geometry) {
    }
}
