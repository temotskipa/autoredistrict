package com.temotskipa.autoredistrict.data;

import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.locationtech.jts.geom.Geometry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

/**
 * Reads Redistricting Data Hub 2020 PL 94-171 tract downloads, for example {@code me_pl2020_t.zip}.
 *
 * <p>Accepted inputs are zips (also nested), folders, shapefiles and CSVs. Counts and geometry may
 * come from one file (RDH's shapefile download) or two (RDH's CSV download plus a boundary
 * shapefile). State and resolution are inferred from the GEOIDs; GEOIDs that lost their leading
 * zero to a numeric read are zero-padded. Population is {@code P0010001} (or the Census API name
 * {@code P1_001N}).
 */
public final class RdhTractImporter {
    
    static final List<String> GEOID_COLUMNS = List.of("GEOID20", "GEOID", "GEOCODE");
    static final List<String> NAME_COLUMNS = List.of("NAME", "NAMELSAD20", "NAMELSAD", "NAME20");
    static final List<String> POP_COLUMNS = List.of("P0010001", "P1_001N");
    
    private RdhTractImporter() {
    }
    
    /**
     * Imports the given files. When {@code expectedFips} or {@code expectedResolution} is given,
     * every input must match it.
     */
    public static List<TractLayer> read(List<Path> inputs, String expectedFips, String expectedResolution) {
        if (inputs.isEmpty()) {
            throw new DataException("No files given to import.");
        }
        Path workdir;
        try {
            workdir = Files.createTempDirectory("rdh-import-");
        } catch (IOException e) {
            throw new UncheckedIOException("Could not create a temporary folder for the import", e);
        }
        try {
            List<Path> files = new ArrayList<>();
            for (Path p : inputs) {
                expand(p, workdir, files);
            }
            List<Layer> layers = new ArrayList<>();
            for (Path f : files) {
                Layer layer = readLayer(f);
                if (layer != null) {
                    layers.add(layer);
                }
            }
            if (layers.isEmpty()) {
                throw new DataException("No tract or block layer with a GEOID20/GEOID column was found. " + "Download the shapefile version of RDH's 2020 PL 94-171 tract file (xx_pl2020_t.zip).");
            }
            Map<String, List<Layer>> groups = new TreeMap<>();
            for (Layer layer : layers) {
                groups.computeIfAbsent(layer.stateFips + "/" + layer.resolution, _ -> new ArrayList<>()).add(layer);
            }
            for (List<Layer> group : groups.values()) {
                Layer first = group.getFirst();
                if (expectedFips != null && !first.stateFips.equals(expectedFips)) {
                    throw new DataException("Files are for state FIPS " + first.stateFips + ", not " + expectedFips + ".");
                }
                if (expectedResolution != null && !first.resolution.equals(expectedResolution)) {
                    throw new DataException("Files are at " + first.resolution + " level, not " + expectedResolution + ".");
                }
                if (!first.resolution.equals("tract")) {
                    throw new DataException("Files for state FIPS " + first.stateFips + " are at " + first.resolution + " level; autoredistrict works on tracts. Import the tract file (xx_pl2020_t.zip).");
                }
            }
            List<TractLayer> out = new ArrayList<>();
            for (List<Layer> group : groups.values()) {
                out.add(join(group));
            }
            return out;
        } finally {
            deleteRecursively(workdir);
        }
    }
    
    private static void expand(Path path, Path workdir, List<Path> found) {
        if (!Files.exists(path)) {
            throw new DataException("File not found: " + path);
        }
        if (Files.isDirectory(path)) {
            try (var s = Files.list(path)) {
                for (Path child : s.sorted().toList()) {
                    expand(child, workdir, found);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Could not list " + path, e);
            }
            return;
        }
        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".zip")) {
            Path target;
            try {
                target = Files.createTempDirectory(workdir, "zip-");
            } catch (IOException e) {
                throw new UncheckedIOException("Could not create a folder for " + path, e);
            }
            unzip(path, target);
            expand(target, workdir, found);
        }
        else if (name.endsWith(".shp") || name.endsWith(".csv")) {
            found.add(path);
        }
    }
    
    static void unzip(Path zip, Path target) {
        try (var in = new ZipInputStream(Files.newInputStream(zip))) {
            ZipEntry entry;
            boolean any = false;
            while ((entry = in.getNextEntry()) != null) {
                any = true;
                Path out = target.resolve(entry.getName()).normalize();
                if (!out.startsWith(target)) {
                    throw new DataException(zip + " has an entry outside the archive folder: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(out);
                }
                else {
                    Files.createDirectories(out.getParent());
                    Files.copy(in, out);
                }
            }
            if (!any) {
                throw new DataException(zip + " is not a valid zip file.");
            }
        } catch (ZipException e) {
            throw new DataException(zip + " is not a valid zip file.", e);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not extract " + zip, e);
        }
    }
    
    private static Layer readLayer(Path path) {
        var lower = path.getFileName().toString().toLowerCase(Locale.ROOT);
        try {
            return lower.endsWith(".csv") ? readCsv(path) : readShapefile(path);
        } catch (DataException e) {
            throw e;
        } catch (Exception e) {
            throw new DataException("Could not read " + path.getFileName() + ": " + e.getMessage(), e);
        }
    }
    
    private static Layer readCsv(Path path) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            var header = Csv.header(reader, path.getFileName().toString());
            var geoidCol = Csv.indexOf(header, GEOID_COLUMNS.toArray(String[]::new));
            if (geoidCol < 0) {
                return null;
            }
            int nameCol = Csv.indexOf(header, NAME_COLUMNS.toArray(String[]::new));
            int popCol = Csv.indexOf(header, POP_COLUMNS.toArray(String[]::new));
            List<String> geoids = new ArrayList<>();
            List<String> names = new ArrayList<>();
            List<Long> pops = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                List<String> f = Csv.split(line);
                geoids.add(f.get(geoidCol));
                names.add(nameCol >= 0 && nameCol < f.size() ? f.get(nameCol) : "");
                if (popCol >= 0) {
                    String raw = popCol < f.size() ? f.get(popCol).strip() : "";
                    pops.add(parseCount(raw, header.get(popCol), path));
                }
            }
            long[] pop = popCol < 0 ? null : pops.stream().mapToLong(Long::longValue).toArray();
            return finish(path, geoids, names, pop, null, null);
        }
    }
    
    private static Layer readShapefile(Path shp) throws IOException {
        var prj = sibling(shp, ".prj");
        var prjWkt = prj == null ? null : Files.readString(prj, StandardCharsets.UTF_8).strip();
        var cpg = sibling(shp, ".cpg");
        var charset = StandardCharsets.ISO_8859_1;
        if (cpg != null) {
            try {
                charset = Charset.forName(Files.readString(cpg).strip());
            } catch (IllegalArgumentException ignored) {
                // Unknown code page name: keep Latin-1.
            }
        }
        var store = new ShapefileDataStore(shp.toUri().toURL());
        try {
            store.setCharset(charset);
            store.setMemoryMapped(false);
            SimpleFeatureType schema = store.getSchema();
            Map<String, String> columns = new HashMap<>();
            for (AttributeDescriptor d : schema.getAttributeDescriptors()) {
                columns.put(d.getLocalName().toUpperCase(Locale.ROOT), d.getLocalName());
            }
            String geoidCol = firstPresent(columns, GEOID_COLUMNS);
            if (geoidCol == null) {
                return null;
            }
            String nameCol = firstPresent(columns, NAME_COLUMNS);
            String popCol = firstPresent(columns, POP_COLUMNS);
            String geomCol = schema.getGeometryDescriptor() == null ? null : schema.getGeometryDescriptor().getLocalName();
            List<String> props = new ArrayList<>(List.of(geoidCol));
            if (nameCol != null) {
                props.add(nameCol);
            }
            if (popCol != null) {
                props.add(popCol);
            }
            if (geomCol != null) {
                props.add(geomCol);
            }
            var query = new Query(schema.getTypeName());
            query.setPropertyNames(props.toArray(String[]::new));
            SimpleFeatureSource source = store.getFeatureSource();
            List<String> geoids = new ArrayList<>();
            List<String> names = new ArrayList<>();
            List<Long> pops = new ArrayList<>();
            List<Geometry> geoms = new ArrayList<>();
            try (var it = source.getFeatures(query).features()) {
                while (it.hasNext()) {
                    SimpleFeature f = it.next();
                    Object id = f.getAttribute(geoidCol);
                    if (id == null) {
                        throw new DataException("Some rows have no GEOID.");
                    }
                    geoids.add(id instanceof Number n ? Long.toString(n.longValue()) : id.toString());
                    names.add(nameCol == null || f.getAttribute(nameCol) == null ? "" : f.getAttribute(nameCol).toString());
                    if (popCol != null) {
                        var v = f.getAttribute(popCol);
                        pops.add(parseCount(v == null ? "" : v.toString(), popCol, shp));
                    }
                    var g = geomCol == null ? null : f.getAttribute(geomCol);
                    geoms.add(g instanceof Geometry geometry ? geometry : null);
                }
            }
            long[] pop = popCol == null ? null : pops.stream().mapToLong(Long::longValue).toArray();
            return finish(shp, geoids, names, pop, geomCol == null ? null : geoms, prjWkt);
        } finally {
            store.dispose();
        }
    }
    
    private static long parseCount(String raw, String column, Path source) {
        var s = raw.strip();
        if (s.endsWith(".0")) {
            s = s.substring(0, s.length() - 2);
        }
        try {
            if (s.isEmpty()) {
                throw new NumberFormatException();
            }
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new DataException(source.getFileName() + " has blank or non-numeric values in " + column + ".");
        }
    }
    
    private static Layer finish(Path source, List<String> rawIds, List<String> names, long[] pop, List<Geometry> geoms, String prjWkt) {
        List<String> ids = new ArrayList<>(rawIds.size());
        int longest = 0;
        for (String raw : rawIds) {
            String s = GeoIds.clean(raw);
            ids.add(s);
            longest = Math.max(longest, s.length());
        }
        // Pad to the tract/block width when the longest id is one short (lost leading zero).
        int width = longest;
        if (longest == GeoIds.TRACT_WIDTH - 1 || longest == GeoIds.BLOCK_WIDTH - 1) {
            width = longest + 1;
        }
        for (int i = 0; i < ids.size(); i++) {
            String s = ids.get(i);
            if (s.length() < width) {
                ids.set(i, "0".repeat(width - s.length()) + s);
            }
        }
        String resolution = GeoIds.resolutionForWidth(width).orElse(null);
        if (resolution == null) {
            return null; // neither tracts nor blocks: not a PL layer (e.g. a county file)
        }
        Set<String> states = new HashSet<>();
        Set<String> seen = new HashSet<>();
        for (String id : ids) {
            states.add(id.substring(0, 2));
            if (!seen.add(id)) {
                throw new DataException(source.getFileName() + " has duplicate GEOIDs.");
            }
        }
        if (states.size() != 1) {
            throw new DataException(source.getFileName() + " covers several states (" + String.join(", ", new TreeSet<>(states)) + "); import one state at a time.");
        }
        return new Layer(source, ids, names, pop, geoms, prjWkt, states.iterator().next(), resolution);
    }
    
    private static TractLayer join(List<Layer> group) {
        Layer counts = group.stream().filter(Layer::hasCounts).findFirst().orElse(null);
        String label = "state " + group.getFirst().stateFips + " at " + group.getFirst().resolution + " level";
        if (counts == null) {
            throw new DataException("No population counts for " + label + ": expected a P0010001 column. " + "Use RDH's PL 94-171 file, not a boundary-only file.");
        }
        Layer geometry = counts.hasGeometry() ? counts : group.stream().filter(Layer::hasGeometry).findFirst().orElse(null);
        if (geometry == null) {
            throw new DataException("No geometry for " + label + ": add the shapefile version of the RDH " + "download, or a boundary shapefile for the same geography.");
        }
        if (geometry.prjWkt == null || geometry.prjWkt.isBlank()) {
            throw new DataException(geometry.source.getFileName() + " has no coordinate system (missing .prj file).");
        }
        Map<String, Geometry> shapes = new LinkedHashMap<>();
        for (int i = 0; i < geometry.geoids.size(); i++) {
            Geometry g = geometry.geometries.get(i);
            if (g != null && !g.isEmpty()) {
                shapes.put(geometry.geoids.get(i), g);
            }
        }
        List<String> warnings = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        for (String id : counts.geoids) {
            if (!shapes.containsKey(id)) {
                missing.add(id);
            }
        }
        if (!missing.isEmpty()) {
            missing.sort(Comparator.naturalOrder());
            throw new DataException(missing.size() + " units for " + label + " have counts but no geometry " + "(for example GEOID " + missing.getFirst() + "). Use geometry from the same RDH download.");
        }
        Set<String> countIds = new HashSet<>(counts.geoids);
        long extra = shapes.keySet().stream().filter(id -> !countIds.contains(id)).count();
        if (extra > 0) {
            warnings.add("Dropped " + extra + " shapes with no population counts.");
        }
        int n = counts.geoids.size();
        var pop = new int[n];
        List<Geometry> geoms = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            long p = counts.pop[i];
            if (p < 0 || p > Integer.MAX_VALUE) {
                throw new DataException(counts.source.getFileName() + " has an impossible population " + p + " for GEOID " + counts.geoids.get(i) + ".");
            }
            pop[i] = (int) p;
            geoms.add(shapes.get(counts.geoids.get(i)));
        }
        List<String> members = new ArrayList<>(new TreeSet<>(List.of(counts.source.getFileName().toString(), geometry.source.getFileName().toString())));
        return new TractLayer(counts.stateFips, List.copyOf(counts.geoids), List.copyOf(counts.names), pop, List.copyOf(geoms), geometry.prjWkt, members, warnings);
    }
    
    private static String firstPresent(Map<String, String> columns, List<String> names) {
        for (String n : names) {
            String actual = columns.get(n.toUpperCase(Locale.ROOT));
            if (actual != null) {
                return actual;
            }
        }
        return null;
    }
    
    private static Path sibling(Path shp, String ext) {
        String base = shp.getFileName().toString();
        base = base.substring(0, base.length() - 4);
        for (String e : List.of(ext, ext.toUpperCase(Locale.ROOT))) {
            Path p = shp.resolveSibling(base + e);
            if (Files.exists(p)) {
                return p;
            }
        }
        return null;
    }
    
    static void deleteRecursively(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        } catch (IOException e) {
            // A leftover temp folder isn't worth failing an import over.
            System.getLogger(RdhTractImporter.class.getName()).log(System.Logger.Level.DEBUG, "Could not delete " + root, e);
        }
    }
    
    /**
     * Opens the first .csv entry of a zip as a stream.
     */
    static InputStream openZipCsv(Path zip) throws IOException {
        var in = new ZipInputStream(Files.newInputStream(zip));
        ZipEntry e;
        while ((e = in.getNextEntry()) != null) {
            if (!e.isDirectory() && e.getName().toLowerCase(Locale.ROOT).endsWith(".csv")) {
                return in;
            }
        }
        in.close();
        throw new DataException(zip.getFileName() + " has no .csv file inside.");
    }
    
    /**
     * One state's tracts: counts joined to lon/lat geometry.
     */
    public record TractLayer(String stateFips, List<String> geoids, List<String> names, int[] pop, List<Geometry> geometries, String geographicWkt, List<String> memberFiles, List<String> warnings) {
        
        public int size() {
            return geoids.size();
        }
        
        public long totalPopulation() {
            long s = 0;
            for (int p : pop) {
                s += p;
            }
            return s;
        }
    }
    
    /**
     * A layer read from one .shp or .csv file.
     */
    private record Layer(Path source, List<String> geoids, List<String> names, long[] pop, List<Geometry> geometries, String prjWkt, String stateFips, String resolution) {
        
        boolean hasCounts() {
            return pop != null;
        }
        
        boolean hasGeometry() {
            return geometries != null && geometries.stream().anyMatch(g -> g != null && !g.isEmpty());
        }
    }
}
