package com.temotskipa.autoredistrict.data;

import com.temotskipa.autoredistrict.data.BlockVotes.Election;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * The import cache, one folder per state under {@code <cache>/rdh/<st>/}:
 * <ul>
 *   <li>{@code manifest.json}: sources with SHA-256s, counts, projection, votes per election;</li>
 *   <li>{@code tracts.csv}: GEOID, name, population, area (m²), centroid (m), perimeter and outer
 *       boundary length (m), in a fixed order that every other file follows;</li>
 *   <li>{@code edges.csv}: adjacency by row index with shared length (m); bridges have length 0;</li>
 *   <li>{@code geometry.bin}: simplified projected tract polygons as length-prefixed WKB, for maps;</li>
 *   <li>{@code votes-2024.csv}, {@code votes-2020.csv}: D and R votes per tract.</li>
 * </ul>
 * Runs read only the cache.
 */
public final class TractCache {
    
    private static final int GEOMETRY_MAGIC = 0x41524731; // "ARG1"
    
    private TractCache() {
    }
    
    public static Path stateDir(Path cacheRoot, StateInfo s) {
        return cacheRoot.resolve("rdh").resolve(s.key());
    }
    
    public static boolean isImported(Path cacheRoot, StateInfo s) {
        return Files.isRegularFile(stateDir(cacheRoot, s).resolve("manifest.json"));
    }
    
    /**
     * Writes a whole state's import, replacing any earlier one.
     */
    public static void write(Path cacheRoot, StateInfo s, Manifest manifest, Tracts tracts, Edges edges, List<Geometry> simplified, Election election, double[] dem, double[] rep) {
        Path dir = stateDir(cacheRoot, s);
        Path tmp = dir.resolveSibling(s.key() + ".tmp");
        try {
            RdhTractImporter.deleteRecursively(tmp);
            Files.createDirectories(tmp);
            writeTracts(tmp.resolve("tracts.csv"), tracts);
            writeEdges(tmp.resolve("edges.csv"), edges);
            writeGeometry(tmp.resolve("geometry.bin"), simplified);
            if (election != null) {
                writeVotes(tmp.resolve(votesFile(election)), tracts.geoid(), dem, rep);
            }
            writeManifest(tmp.resolve("manifest.json"), manifest);
            RdhTractImporter.deleteRecursively(dir);
            Files.move(tmp, dir, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write the cache for " + s.abbr() + " in " + dir, e);
        }
    }
    
    /**
     * Adds or replaces one election's votes for an imported state.
     */
    public static void writeVotes(Path cacheRoot, StateInfo s, Election election, double[] dem, double[] rep, Manifest.VoteInfo info) {
        Path dir = stateDir(cacheRoot, s);
        Manifest manifest = readManifest(cacheRoot, s);
        Tracts tracts = readTracts(dir);
        try {
            Path tmp = dir.resolve(votesFile(election) + ".tmp");
            writeVotes(tmp, tracts.geoid(), dem, rep);
            Files.move(tmp, dir.resolve(votesFile(election)), StandardCopyOption.REPLACE_EXISTING);
            Path mtmp = dir.resolve("manifest.json.tmp");
            writeManifest(mtmp, manifest.withVotes(election.label(), info));
            Files.move(mtmp, dir.resolve("manifest.json"), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write " + election.label() + " votes for " + s.abbr(), e);
        }
    }
    
    static String votesFile(Election e) {
        return "votes-" + e.label() + ".csv";
    }
    
    public static Manifest readManifest(Path cacheRoot, StateInfo s) {
        Path file = stateDir(cacheRoot, s).resolve("manifest.json");
        if (!Files.isRegularFile(file)) {
            throw new DataException(s.abbr() + " isn't imported yet. Run: autoredistrict import --rdh-dir <folder> " + "--states " + s.abbr());
        }
        try (var in = Files.newInputStream(file)) {
            Manifest m = Json.mapper().readValue(in, Manifest.class);
            if (m.format() != Manifest.FORMAT) {
                throw new DataException("The cache for " + s.abbr() + " has format " + m.format() + "; re-import it.");
            }
            return m;
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + file, e);
        }
    }
    
    private static void writeManifest(Path file, Manifest m) throws IOException {
        try (OutputStream out = Files.newOutputStream(file)) {
            Json.mapper().writeValue(out, m);
        }
    }
    
    private static void writeTracts(Path file, Tracts t) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            w.write("geoid,name,pop,area_m2,x_m,y_m,perimeter_m,outer_m\n");
            for (int i = 0; i < t.size(); i++) {
                w.write(t.geoid()[i] + "," + Csv.quote(t.name()[i]) + "," + t.pop()[i] + "," + Csv.num(t.area()[i]) + "," + Csv.num(t.x()[i]) + "," + Csv.num(t.y()[i]) + "," + Csv.num(t.perimeter()[i]) + "," + Csv.num(t.outer()[i]) + "\n");
            }
        }
    }
    
    public static Tracts readTracts(Path dir) {
        Path file = dir.resolve("tracts.csv");
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            Csv.header(r, file.toString());
            String line;
            while ((line = r.readLine()) != null) {
                if (!line.isEmpty()) {
                    rows.add(Csv.split(line).toArray(String[]::new));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + file, e);
        }
        int n = rows.size();
        var t = new Tracts(new String[n], new String[n], new int[n], new double[n], new double[n], new double[n], new double[n], new double[n]);
        for (int i = 0; i < n; i++) {
            String[] f = rows.get(i);
            t.geoid()[i] = f[0];
            t.name()[i] = f[1];
            t.pop()[i] = Integer.parseInt(f[2]);
            t.area()[i] = Double.parseDouble(f[3]);
            t.x()[i] = Double.parseDouble(f[4]);
            t.y()[i] = Double.parseDouble(f[5]);
            t.perimeter()[i] = Double.parseDouble(f[6]);
            t.outer()[i] = Double.parseDouble(f[7]);
        }
        return t;
    }
    
    private static void writeEdges(Path file, Edges e) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            w.write("a,b,length_m,bridge\n");
            for (int i = 0; i < e.a().length; i++) {
                w.write(e.a()[i] + "," + e.b()[i] + "," + Csv.num(e.length()[i]) + "," + (e.bridge()[i] ? 1 : 0) + "\n");
            }
        }
    }
    
    public static Edges readEdges(Path dir) {
        Path file = dir.resolve("edges.csv");
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            Csv.header(r, file.toString());
            String line;
            while ((line = r.readLine()) != null) {
                if (!line.isEmpty()) {
                    rows.add(line.split(","));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + file, e);
        }
        int m = rows.size();
        var e = new Edges(new int[m], new int[m], new double[m], new boolean[m]);
        for (int i = 0; i < m; i++) {
            String[] f = rows.get(i);
            e.a()[i] = Integer.parseInt(f[0]);
            e.b()[i] = Integer.parseInt(f[1]);
            e.length()[i] = Double.parseDouble(f[2]);
            e.bridge()[i] = f[3].equals("1");
        }
        return e;
    }
    
    private static void writeGeometry(Path file, List<Geometry> geoms) throws IOException {
        var writer = new WKBWriter(2);
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file), 1 << 16))) {
            out.writeInt(GEOMETRY_MAGIC);
            out.writeInt(geoms.size());
            for (Geometry g : geoms) {
                byte[] wkb = writer.write(g);
                out.writeInt(wkb.length);
                out.write(wkb);
            }
        }
    }
    
    /**
     * Reads the simplified projected tract polygons, in tract order.
     */
    public static List<Geometry> readGeometry(Path dir) {
        Path file = dir.resolve("geometry.bin");
        var reader = new WKBReader(new GeometryFactory());
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(file), 1 << 16))) {
            if (in.readInt() != GEOMETRY_MAGIC) {
                throw new DataException(file + " isn't an autoredistrict geometry file; re-import the state.");
            }
            int n = in.readInt();
            List<Geometry> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                var wkb = new byte[in.readInt()];
                in.readFully(wkb);
                out.add(reader.read(wkb));
            }
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + file, e);
        } catch (ParseException e) {
            throw new DataException(file + " is corrupt; re-import the state.", e);
        }
    }
    
    private static void writeVotes(Path file, String[] geoids, double[] dem, double[] rep) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            w.write("geoid,dem,rep\n");
            for (int i = 0; i < geoids.length; i++) {
                w.write(geoids[i] + "," + Csv.num(dem[i]) + "," + Csv.num(rep[i]) + "\n");
            }
        }
    }
    
    /**
     * Reads cached votes for an election, or returns null if that election isn't cached.
     */
    public static double[][] readVotes(Path dir, Election election, String[] geoids) {
        Path file = dir.resolve(votesFile(election));
        if (!Files.isRegularFile(file)) {
            return null;
        }
        var dem = new double[geoids.length];
        var rep = new double[geoids.length];
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            Csv.header(r, file.toString());
            String line;
            int i = 0;
            while ((line = r.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String[] f = line.split(",");
                if (i >= geoids.length || !f[0].equals(geoids[i])) {
                    throw new DataException(file + " doesn't match tracts.csv; re-import the state.");
                }
                dem[i] = Double.parseDouble(f[1]);
                rep[i] = Double.parseDouble(f[2]);
                i++;
            }
            if (i != geoids.length) {
                throw new DataException(file + " has " + i + " rows for " + geoids.length + " tracts; re-import.");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + file, e);
        }
        return new double[][]{dem, rep};
    }
    
    /**
     * Tract table as written to tracts.csv.
     */
    public value record Tracts(String[] geoid, String[] name, int[] pop, double[] area, double[] x, double[] y, double[] perimeter, double[] outer) {
        
        public int size() {
            return geoid.length;
        }
    }
    
    /**
     * Edge table as written to edges.csv.
     */
    public value record Edges(int[] a, int[] b, double[] length, boolean[] bridge) {
    }
}
