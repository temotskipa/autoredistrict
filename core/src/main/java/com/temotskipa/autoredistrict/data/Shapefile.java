package com.temotskipa.autoredistrict.data;

import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.algorithm.RayCrossingCounter;
import org.locationtech.jts.geom.*;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.foreign.ValueLayout.OfDouble;
import java.lang.foreign.ValueLayout.OfInt;
import java.lang.foreign.ValueLayout.OfShort;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;

/**
 * ESRI shapefiles of polygons with a dBASE III attribute table: what RDH ships and what the
 * district export writes. Polygon, PolygonZ and PolygonM shapes are read (Z and M are dropped);
 * points and lines aren't supported.
 *
 * <p>Reading and writing map the files with the foreign memory API. Reading builds every shape
 * as a JTS MultiPolygon the way the format defines rings: clockwise rings are shells, counter-clockwise
 * rings are holes of the smallest shell around them, and a hole with no shell around it becomes a
 * shell. Rows the table marks deleted are skipped together with their shapes. An instance holds
 * the mapped files until it is closed, on the thread that opened it.
 */
public value class Shapefile implements AutoCloseable {
    
    private static final int FILE_CODE = 9994;
    private static final int VERSION = 1000;
    private static final int NULL_SHAPE = 0;
    private static final int POLYGON = 5;
    // Polygon, PolygonZ and PolygonM.
    private static final Set<Integer> POLYGON_TYPES = Set.of(POLYGON, 15, 25);
    private static final int HEADER = 100;
    
    private static final OfInt INT_BE = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final OfInt INT_LE = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final OfShort SHORT_LE = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final OfDouble DOUBLE_LE = ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    
    private static final GeometryFactory GEOMETRY = new GeometryFactory();
    
    private final String name;
    private final Arena arena;
    private final MemorySegment shapes;
    private final MemorySegment table;
    private final Charset charset;
    private final String prj;
    private final List<Column> columns;
    private final int rows;
    private final long headerLength;
    private final long recordLength;
    
    private Shapefile(Path shp, Arena arena, MemorySegment shapes, MemorySegment table, Charset charset, String prj) {
        this.name = shp.getFileName().toString();
        this.arena = arena;
        this.shapes = shapes;
        this.table = table;
        this.charset = charset;
        this.prj = prj;
        List<Column> cols = new ArrayList<>();
        if (table == null) {
            rows = 0;
            headerLength = 0;
            recordLength = 0;
        }
        else {
            if (table.byteSize() < 32) {
                throw damaged(name, "its .dbf header is cut short");
            }
            rows = table.get(INT_LE, 4);
            headerLength = Short.toUnsignedInt(table.get(SHORT_LE, 8));
            recordLength = Short.toUnsignedInt(table.get(SHORT_LE, 10));
            int offset = 1; // after the deletion flag
            for (long p = 32; p + 32 <= headerLength && table.get(ValueLayout.JAVA_BYTE, p) != 0x0D; p += 32) {
                byte[] raw = table.asSlice(p, 11).toArray(ValueLayout.JAVA_BYTE);
                int end = 0;
                while (end < raw.length && raw[end] != 0) {
                    end++;
                }
                char type = (char) (table.get(ValueLayout.JAVA_BYTE, p + 11) & 0xFF);
                int length = table.get(ValueLayout.JAVA_BYTE, p + 16) & 0xFF;
                cols.add(new Column(new String(raw, 0, end, StandardCharsets.ISO_8859_1).trim(), type, offset, length));
                offset += length;
            }
            if (rows < 0 || offset > recordLength || headerLength + rows * recordLength > table.byteSize()) {
                throw damaged(name, "its .dbf is cut short or has a bad header");
            }
        }
        this.columns = List.copyOf(cols);
    }
    
    /**
     * Opens {@code shp} and its {@code .dbf}, {@code .prj} and {@code .cpg} siblings (each
     * optional). Text is decoded with the {@code .cpg} code page, else Latin-1.
     */
    public static Shapefile open(Path shp) throws IOException {
        Path dbf = sibling(shp, ".dbf");
        Path prj = sibling(shp, ".prj");
        Path cpg = sibling(shp, ".cpg");
        Charset charset = StandardCharsets.ISO_8859_1;
        if (cpg != null) {
            try {
                charset = Charset.forName(Files.readString(cpg).strip());
            } catch (IllegalArgumentException ignored) {
                // Unknown code page name: keep Latin-1.
            }
        }
        String wkt = prj == null ? null : Files.readString(prj, StandardCharsets.UTF_8).strip();
        var arena = Arena.ofConfined();
        try {
            return new Shapefile(shp, arena, map(shp, arena), dbf == null ? null : map(dbf, arena), charset, wkt);
        } catch (IOException | RuntimeException e) {
            arena.close();
            throw e;
        }
    }
    
    /**
     * The {@code .prj} WKT, or null without one.
     */
    public String prj() {
        return prj;
    }
    
    /**
     * The attribute table's column names in file order; empty without a {@code .dbf}.
     */
    public List<String> columns() {
        return columns.stream().map(Column::name).toList();
    }
    
    /**
     * One column's values for every row, as trimmed text. Blank numbers and text cells that start
     * with a NUL byte are null.
     */
    public List<String> column(String columnName) {
        Column col = columns.stream().filter(c -> c.name().equals(columnName)).findFirst().orElseThrow(() -> new IllegalArgumentException(name + " has no column " + columnName));
        List<String> out = new ArrayList<>(rows);
        for (int r = 0; r < rows; r++) {
            if (kept(r)) {
                out.add(cell(col, row(r) + col.offset()));
            }
        }
        return out;
    }
    
    /**
     * Every row's shape; an empty MultiPolygon for a null shape.
     */
    public List<Geometry> geometries() {
        if (shapes.byteSize() < HEADER || shapes.get(INT_BE, 0) != FILE_CODE) {
            throw new DataException(name + " isn't a shapefile.");
        }
        int type = shapes.get(INT_LE, 32);
        if (type != NULL_SHAPE && !POLYGON_TYPES.contains(type)) {
            throw new DataException(name + " has shapes of type " + type + "; tract files have polygons (type 5).");
        }
        long end = Math.min(shapes.byteSize(), 2L * Integer.toUnsignedLong(shapes.get(INT_BE, 24)));
        List<Geometry> all = new ArrayList<>();
        for (long p = HEADER; p + 8 <= end; ) {
            long content = p + 8;
            long length = 2L * shapes.get(INT_BE, p + 4);
            if (length < 4 || content + length > shapes.byteSize()) {
                throw damaged(name, "shape " + (all.size() + 1) + " is cut short");
            }
            all.add(shape(content, length, all.size() + 1));
            p = content + length;
        }
        if (table == null) {
            return all;
        }
        if (all.size() != rows) {
            throw new DataException(name + " has " + all.size() + " shapes but its .dbf has " + rows + " rows.");
        }
        List<Geometry> kept = new ArrayList<>(rows);
        for (int r = 0; r < rows; r++) {
            if (kept(r)) {
                kept.add(all.get(r));
            }
        }
        return kept;
    }
    
    @Override
    public void close() {
        arena.close();
    }
    
    /**
     * Writes {@code shp} with its {@code .shx} index, a UTF-8 {@code .dbf} table (with a
     * {@code .cpg} saying so) and a {@code .prj} holding {@code prjWkt}. Geometries must be polygons
     * or multipolygons (null or empty ones are written as null shapes); shells are written
     * clockwise and holes counter-clockwise, as the format asks.
     */
    public static void write(Path shp, String prjWkt, List<Field> fields, List<Feature> features) throws IOException {
        String base = baseName(shp);
        for (Feature feature : features) {
            if (feature.values().size() != fields.size()) {
                throw new IllegalArgumentException("A feature has " + feature.values().size() + " values for " + fields.size() + " columns.");
            }
        }
        var byFeature = features.stream().map(f -> polygons(f.geometry())).toList();
        var bounds = new Envelope();
        long shpBytes = HEADER;
        for (List<Polygon> shape : byFeature) {
            shape.forEach(p -> bounds.expandToInclude(p.getEnvelopeInternal()));
            shpBytes += 8 + contentLength(shape);
        }
        long shxBytes = HEADER + 8L * features.size();
        int recordLength = 1 + fields.stream().mapToInt(Field::length).sum();
        int headerLength = 32 + 32 * fields.size() + 1;
        Files.createDirectories(shp.toAbsolutePath().getParent());
        try (var arena = Arena.ofConfined()) {
            var shpOut = new Cursor(create(shp, shpBytes, arena));
            var shxOut = new Cursor(create(shp.resolveSibling(base + ".shx"), shxBytes, arena));
            var dbfOut = new Cursor(create(shp.resolveSibling(base + ".dbf"), headerLength + (long) recordLength * features.size() + 1, arena));
            header(shpOut, shpBytes, bounds);
            header(shxOut, shxBytes, bounds);
            for (int i = 0; i < byFeature.size(); i++) {
                List<Polygon> shape = byFeature.get(i);
                int length = contentLength(shape);
                shxOut.intBE(Math.toIntExact(shpOut.position() / 2)).intBE(length / 2);
                shpOut.intBE(i + 1).intBE(length / 2);
                content(shpOut, shape);
            }
            table(dbfOut, headerLength, recordLength, fields, features);
            shpOut.finish();
            shxOut.finish();
            dbfOut.finish();
        }
        Files.writeString(shp.resolveSibling(base + ".cpg"), "UTF-8");
        if (prjWkt != null) {
            Files.writeString(shp.resolveSibling(base + ".prj"), prjWkt);
        }
    }
    
    private Geometry shape(long content, long length, int number) {
        int type = shapes.get(INT_LE, content);
        if (type == NULL_SHAPE) {
            return GEOMETRY.createMultiPolygon();
        }
        if (!POLYGON_TYPES.contains(type)) {
            throw new DataException(name + " has a shape of type " + type + " (shape " + number + "); tract files have polygons (type 5).");
        }
        if (length < 44) {
            throw damaged(name, "shape " + number + " is cut short");
        }
        int parts = shapes.get(INT_LE, content + 36);
        int points = shapes.get(INT_LE, content + 40);
        long partsAt = content + 44;
        long pointsAt = partsAt + 4L * parts;
        if (parts < 0 || points < 0 || pointsAt + 16L * points > content + length) {
            throw damaged(name, "shape " + number + " has more points than it has room for");
        }
        List<LinearRing> shells = new ArrayList<>();
        List<LinearRing> holes = new ArrayList<>();
        for (int k = 0; k < parts; k++) {
            int start = shapes.get(INT_LE, partsAt + 4L * k);
            int finish = k == parts - 1 ? points : shapes.get(INT_LE, partsAt + 4L * (k + 1));
            if (start < 0 || start > finish || finish > points) {
                throw damaged(name, "shape " + number + " has bad ring offsets");
            }
            int count = finish - start;
            if (count == 0) {
                continue;
            }
            double x0 = x(pointsAt, start);
            double y0 = y(pointsAt, start);
            boolean close = x0 != x(pointsAt, finish - 1) || y0 != y(pointsAt, finish - 1);
            var coords = new Coordinate[count + (close ? 1 : 0)];
            for (int i = 0; i < count; i++) {
                coords[i] = new Coordinate(x(pointsAt, start + i), y(pointsAt, start + i));
            }
            if (close) {
                coords[count] = new Coordinate(x0, y0);
            }
            // Rings of fewer than four points enclose nothing.
            if (coords.length > 3) {
                LinearRing ring = GEOMETRY.createLinearRing(coords);
                (Orientation.isCCW(ring.getCoordinateSequence()) ? holes : shells).add(ring);
            }
        }
        if (shells.size() == 1) {
            return GEOMETRY.createMultiPolygon(new Polygon[]{GEOMETRY.createPolygon(shells.getFirst(), holes.toArray(LinearRing[]::new))});
        }
        if (shells.isEmpty() && holes.size() == 1) {
            return GEOMETRY.createMultiPolygon(new Polygon[]{GEOMETRY.createPolygon(holes.getFirst())});
        }
        List<List<LinearRing>> holesOf = assignHoles(shells, holes);
        var built = new Polygon[shells.size()];
        for (int i = 0; i < built.length; i++) {
            built[i] = GEOMETRY.createPolygon(shells.get(i), holesOf.get(i).toArray(LinearRing[]::new));
        }
        return GEOMETRY.createMultiPolygon(built);
    }
    
    /**
     * For each shell, the holes whose smallest enclosing shell it is. A hole inside no shell is
     * appended to {@code shells} as a shell of its own.
     */
    private static List<List<LinearRing>> assignHoles(List<LinearRing> shells, List<LinearRing> holes) {
        List<List<LinearRing>> holesOf = new ArrayList<>();
        for (int i = 0; i < shells.size(); i++) {
            holesOf.add(new ArrayList<>());
        }
        for (LinearRing hole : holes) {
            Envelope holeBounds = hole.getEnvelopeInternal();
            Coordinate probe = hole.getCoordinateN(0);
            int best = -1;
            for (int i = 0; i < shells.size(); i++) {
                Envelope shellBounds = shells.get(i).getEnvelopeInternal();
                Coordinate[] ring = shells.get(i).getCoordinates();
                boolean inside = shellBounds.contains(holeBounds) && (RayCrossingCounter.locatePointInRing(probe, ring) != Location.EXTERIOR || isVertex(probe, ring));
                if (inside && (best < 0 || shells.get(best).getEnvelopeInternal().contains(shellBounds))) {
                    best = i;
                }
            }
            if (best < 0) {
                shells.add(hole);
                holesOf.add(new ArrayList<>());
            }
            else {
                holesOf.get(best).add(hole);
            }
        }
        return holesOf;
    }
    
    private static boolean isVertex(Coordinate p, Coordinate[] ring) {
        for (Coordinate c : ring) {
            if (c.x == p.x && c.y == p.y) {
                return true;
            }
        }
        return false;
    }
    
    private double x(long pointsAt, int i) {
        return shapes.get(DOUBLE_LE, pointsAt + 16L * i);
    }
    
    private double y(long pointsAt, int i) {
        return shapes.get(DOUBLE_LE, pointsAt + 16L * i + 8);
    }
    
    private long row(int r) {
        return headerLength + r * recordLength;
    }
    
    private boolean kept(int r) {
        return table.get(ValueLayout.JAVA_BYTE, row(r)) != '*';
    }
    
    private String cell(Column col, long at) {
        byte first = table.get(ValueLayout.JAVA_BYTE, at);
        byte[] raw = table.asSlice(at, col.length()).toArray(ValueLayout.JAVA_BYTE);
        return switch (Character.toUpperCase(col.type())) {
            case 'C' -> first == 0 ? null : new String(raw, charset).trim();
            case 'N', 'F' -> {
                String s = new String(raw, StandardCharsets.ISO_8859_1).trim();
                yield first == '*' || s.isEmpty() ? null : s;
            }
            default -> {
                String s = new String(raw, charset).trim();
                yield s.isEmpty() ? null : s;
            }
        };
    }
    
    private static DataException damaged(String file, String what) {
        return new DataException(file + " is damaged: " + what + ".");
    }
    
    /**
     * The file name without its {@code .shp} extension.
     */
    private static String baseName(Path shp) {
        String file = shp.getFileName().toString();
        if (!file.toLowerCase(Locale.ROOT).endsWith(".shp")) {
            throw new IllegalArgumentException("A shapefile's name ends in .shp: " + file);
        }
        return file.substring(0, file.length() - 4);
    }
    
    private static MemorySegment map(Path file, Arena arena) throws IOException {
        try (var channel = FileChannel.open(file, StandardOpenOption.READ)) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
        }
    }
    
    private static Path sibling(Path shp, String ext) {
        String base = baseName(shp);
        for (String e : List.of(ext, ext.toUpperCase(Locale.ROOT))) {
            Path p = shp.resolveSibling(base + e);
            if (Files.exists(p)) {
                return p;
            }
        }
        return null;
    }
    
    private static List<Polygon> polygons(Geometry g) {
        return switch (g) {
            case null -> List.of();
            case Polygon p -> p.isEmpty() ? List.of() : List.of(p);
            case MultiPolygon m -> {
                List<Polygon> out = new ArrayList<>();
                for (int i = 0; i < m.getNumGeometries(); i++) {
                    if (!m.getGeometryN(i).isEmpty()) {
                        out.add((Polygon) m.getGeometryN(i));
                    }
                }
                yield out;
            }
            default ->
                    throw new IllegalArgumentException("A shapefile here holds polygons, not a " + g.getGeometryType() + ".");
        };
    }
    
    private static List<Coordinate[]> rings(List<Polygon> shape) {
        List<Coordinate[]> out = new ArrayList<>();
        for (Polygon p : shape) {
            out.add(oriented(p.getExteriorRing(), false));
            for (int h = 0; h < p.getNumInteriorRing(); h++) {
                out.add(oriented(p.getInteriorRingN(h), true));
            }
        }
        return out;
    }
    
    private static Coordinate[] oriented(LinearRing ring, boolean counterClockwise) {
        Coordinate[] c = ring.getCoordinates();
        if (Orientation.isCCW(c) == counterClockwise) {
            return c;
        }
        Coordinate[] reversed = c.clone();
        CoordinateArrays.reverse(reversed);
        return reversed;
    }
    
    private static int contentLength(List<Polygon> shape) {
        if (shape.isEmpty()) {
            return 4;
        }
        List<Coordinate[]> rings = rings(shape);
        int points = rings.stream().mapToInt(r -> r.length).sum();
        return 44 + 4 * rings.size() + 16 * points;
    }
    
    private static void header(Cursor out, long bytes, Envelope bounds) {
        out.intBE(FILE_CODE).zeros(20).intBE(Math.toIntExact(bytes / 2)).intLE(VERSION).intLE(POLYGON);
        box(out, bounds);
        // Z and M ranges.
        out.zeros(32);
    }
    
    private static void box(Cursor out, Envelope e) {
        if (e.isNull()) {
            out.zeros(32);
        }
        else {
            out.doubleLE(e.getMinX()).doubleLE(e.getMinY()).doubleLE(e.getMaxX()).doubleLE(e.getMaxY());
        }
    }
    
    private static void content(Cursor out, List<Polygon> shape) {
        if (shape.isEmpty()) {
            out.intLE(NULL_SHAPE);
            return;
        }
        var rings = rings(shape);
        var bounds = new Envelope();
        shape.forEach(p -> bounds.expandToInclude(p.getEnvelopeInternal()));
        out.intLE(POLYGON);
        box(out, bounds);
        out.intLE(rings.size()).intLE(rings.stream().mapToInt(r -> r.length).sum());
        int start = 0;
        for (Coordinate[] ring : rings) {
            out.intLE(start);
            start += ring.length;
        }
        for (Coordinate[] ring : rings) {
            for (Coordinate c : ring) {
                out.doubleLE(c.x).doubleLE(c.y);
            }
        }
    }
    
    /**
     * A dBASE III table: version, today's date, row count, header and row lengths, a 32-byte
     * descriptor per column (name, type, length, decimals), then the rows, each after a blank
     * deletion flag.
     */
    private static void table(Cursor out, int headerLength, int recordLength, List<Field> fields, List<Feature> features) {
        var today = LocalDate.now(ZoneOffset.UTC);
        out.u8(0x03).u8(today.getYear() - 1900).u8(today.getMonthValue()).u8(today.getDayOfMonth());
        out.intLE(features.size()).shortLE(headerLength).shortLE(recordLength).zeros(20);
        for (Field f : fields) {
            out.bytes(Arrays.copyOf(f.name().getBytes(StandardCharsets.US_ASCII), 11)).u8(f.type()).zeros(4).u8(f.length()).u8(f.decimals()).zeros(14);
        }
        out.u8(0x0D);
        for (Feature feature : features) {
            out.u8(' ');
            for (int i = 0; i < fields.size(); i++) {
                out.bytes(encode(fields.get(i), feature.values().get(i)));
            }
        }
        out.u8(0x1A);
    }
    
    private static MemorySegment create(Path file, long size, Arena arena) throws IOException {
        try (var channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            return channel.map(MapMode.READ_WRITE, 0, size, arena);
        }
    }
    
    private static byte[] encode(Field f, Object value) {
        var out = new byte[f.length()];
        Arrays.fill(out, (byte) ' ');
        if (value == null) {
            return out;
        }
        if (f.type() == 'C') {
            byte[] text = fitted(value.toString(), f.length());
            System.arraycopy(text, 0, out, 0, text.length);
            return out;
        }
        String s = switch (value) {
            case Long l when f.decimals() == 0 -> Long.toString(l);
            case Integer i when f.decimals() == 0 -> Integer.toString(i);
            // The exact value of the double, rounded half up, as Formatter's %.nf does.
            case Number x when Double.isFinite(x.doubleValue()) ->
                    new BigDecimal(x.doubleValue()).setScale(f.decimals(), RoundingMode.HALF_UP).toPlainString();
            default -> throw new IllegalArgumentException(f.name() + " needs a finite number, not " + value + ".");
        };
        if (s.length() > f.length()) {
            throw new IllegalArgumentException(f.name() + " value " + s + " is wider than its " + f.length() + " characters.");
        }
        byte[] text = s.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(text, 0, out, f.length() - text.length, text.length);
        return out;
    }
    
    /**
     * UTF-8 bytes of {@code s}, cut to at most {@code length} without splitting a character.
     */
    private static byte[] fitted(String s, int length) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        if (b.length <= length) {
            return b;
        }
        int end = length;
        while (end > 0 && (b[end] & 0xC0) == 0x80) {
            end--;
        }
        return Arrays.copyOf(b, end);
    }
    
    /**
     * Writes values one after another into a segment, here a mapped output file.
     */
    private static final class Cursor {
        
        private final MemorySegment segment;
        private long at;
        
        Cursor(MemorySegment segment) {
            this.segment = segment;
        }
        
        long position() {
            return at;
        }
        
        Cursor u8(int v) {
            segment.set(ValueLayout.JAVA_BYTE, at, (byte) v);
            at += 1;
            return this;
        }
        
        Cursor shortLE(int v) {
            segment.set(SHORT_LE, at, (short) v);
            at += 2;
            return this;
        }
        
        Cursor intBE(int v) {
            segment.set(INT_BE, at, v);
            at += 4;
            return this;
        }
        
        Cursor intLE(int v) {
            segment.set(INT_LE, at, v);
            at += 4;
            return this;
        }
        
        Cursor doubleLE(double v) {
            segment.set(DOUBLE_LE, at, v);
            at += 8;
            return this;
        }
        
        Cursor bytes(byte[] b) {
            MemorySegment.copy(b, 0, segment, ValueLayout.JAVA_BYTE, at, b.length);
            at += b.length;
            return this;
        }
        
        Cursor zeros(long n) {
            segment.asSlice(at, n).fill((byte) 0);
            at += n;
            return this;
        }
        
        /**
         * Checks that the precomputed file size was filled exactly.
         */
        void finish() {
            if (at != segment.byteSize()) {
                throw new IllegalStateException("Wrote " + at + " of " + segment.byteSize() + " bytes.");
            }
        }
    }
    
    /**
     * A dBASE column: text ({@code C}) or a number ({@code N}) of a fixed width.
     */
    public value record Field(String name, char type, int length, int decimals) {
        public Field {
            if (name.isEmpty() || name.length() > 10 || !name.chars().allMatch(ch -> ch > ' ' && ch < 127)) {
                throw new IllegalArgumentException("A dBASE column name is 1 to 10 ASCII characters, not '" + name + "'.");
            }
            if (type != 'C' && type != 'N') {
                throw new IllegalArgumentException("Column " + name + " is text (C) or a number (N), not " + type + ".");
            }
            if (length < 1 || length > 254 || decimals < 0 || (decimals > 0 && decimals > length - 2)) {
                throw new IllegalArgumentException("Column " + name + " can't be " + length + " characters with " + decimals + " decimals.");
            }
        }
        
        public static Field text(String name, int length) {
            return new Field(name, 'C', length, 0);
        }
        
        public static Field number(String name, int length, int decimals) {
            return new Field(name, 'N', length, decimals);
        }
    }
    
    /**
     * One shape and its values, in column order.
     */
    public value record Feature(Geometry geometry, List<?> values) {
    }
    
    private value record Column(String name, char type, int offset, int length) {
    }
}
