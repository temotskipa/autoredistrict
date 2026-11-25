package com.temotskipa.autoredistrict.data;

import com.temotskipa.autoredistrict.data.BlockVotes.Election;
import com.temotskipa.autoredistrict.data.BlockVotes.TractVotes;
import org.locationtech.jts.coverage.CoverageSimplifier;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.Consumer;

/**
 * Imports RDH downloads into the cache: tract geometry and population, the projection, adjacency,
 * connectivity repair, simplified map geometry, and votes for 2024 (per-state files) and 2020
 * (the national file, read once for all states).
 */
public final class StateImporter {
    
    /**
     * Map geometry is simplified as a coverage to this tolerance; shared edges stay shared.
     */
    public static final double SIMPLIFY_METRES = 25.0;
    
    private StateImporter() {
    }
    
    /**
     * Imports one state's tract zip and (if given) its 2024 vote zip.
     */
    public static ImportSummary importState(StateInfo s, Path tractZip, Path votes2024Zip, Path cacheRoot) {
        long t0 = System.nanoTime();
        RdhTractImporter.TractLayer layer = single(RdhTractImporter.read(List.of(tractZip), s.fips(), "tract"), s);
        if (!layer.geographicWkt().startsWith("GEOGCS")) {
            throw new DataException(tractZip.getFileName() + " isn't in geographic coordinates (its .prj is not a " + "GEOGCS); RDH tract files are NAD83 longitude/latitude.");
        }
        AlbersProjection proj = AlbersProjection.forBounds(AlbersProjection.unwrappedBounds(layer.geometries()), layer.geographicWkt());
        int n = layer.size();
        List<Geometry> projected = new ArrayList<>(n);
        for (Geometry g : layer.geometries()) {
            projected.add(proj.project(g));
        }
        AdjacencyBuilder.Adjacency adj = AdjacencyBuilder.build(layer.geometries(), projected);
        Connectivity.Repair repair = Connectivity.connect(projected, adj.a(), adj.b());
        
        int m = adj.edgeCount() + repair.bridges().size();
        var edges = new TractCache.Edges(new int[m], new int[m], new double[m], new boolean[m]);
        for (int e = 0; e < adj.edgeCount(); e++) {
            edges.a()[e] = adj.a()[e];
            edges.b()[e] = adj.b()[e];
            edges.length()[e] = adj.length()[e];
        }
        List<Manifest.BridgeInfo> bridgeInfo = new ArrayList<>();
        for (int i = 0; i < repair.bridges().size(); i++) {
            Connectivity.Bridge b = repair.bridges().get(i);
            int e = adj.edgeCount() + i;
            edges.a()[e] = b.a();
            edges.b()[e] = b.b();
            edges.bridge()[e] = true;
            bridgeInfo.add(new Manifest.BridgeInfo(layer.geoids().get(b.a()), layer.geoids().get(b.b()), Math.round(b.gapMetres() * 10) / 10.0));
        }
        
        var tracts = new TractCache.Tracts(layer.geoids().toArray(String[]::new), layer.names().toArray(String[]::new), layer.pop().clone(), new double[n], new double[n], new double[n], adj.perimeter(), adj.outer());
        for (int i = 0; i < n; i++) {
            Geometry g = projected.get(i);
            tracts.area()[i] = g.getArea();
            Point c = g.getCentroid();
            if (c.isEmpty()) {
                c = g.getInteriorPoint();
            }
            tracts.x()[i] = c.getX();
            tracts.y()[i] = c.getY();
        }
        List<Geometry> simplified = simplify(projected);
        
        List<String> warnings = new ArrayList<>(layer.warnings());
        TractVotes votes;
        Manifest.VoteInfo voteInfo = null;
        double[] dem = null;
        double[] rep = null;
        if (votes2024Zip != null) {
            Set<String> ids = new HashSet<>(layer.geoids());
            try (InputStream in = BlockVotes.openCsv(votes2024Zip)) {
                votes = BlockVotes.read2024(in, votes2024Zip.getFileName().toString(), s.fips(), ids);
            } catch (IOException e) {
                throw new UncheckedIOException("Could not read " + votes2024Zip, e);
            }
            requireVotes(votes, votes2024Zip.getFileName().toString(), s);
            dem = new double[n];
            rep = new double[n];
            fill(votes, layer.geoids(), dem, rep);
            long missing = votes.tractsMissing(layer.geoids());
            String warn = null;
            if (votes.blocksOutside() > 0) {
                warn = votes.blocksOutside() + " blocks in " + votes2024Zip.getFileName() + " fall outside the state's tracts and were skipped.";
                warnings.add(warn);
            }
            voteInfo = new Manifest.VoteInfo("2024", votes2024Zip.getFileName().toString(), Json.sha256(votes2024Zip), votes.dem(), votes.rep(), votes.demShare(), votes.blocks(), votes.blocksOutside(), missing, null, warn);
        }
        
        var pinfo = new Manifest.ProjectionInfo(proj.standardParallel1(), proj.standardParallel2(), proj.latitudeOfCenter(), proj.longitudeOfCenter(), proj.geographicWkt(), proj.wkt());
        Map<String, Manifest.VoteInfo> voteMap = new TreeMap<>();
        if (voteInfo != null) {
            voteMap.put("2024", voteInfo);
        }
        var manifest = new Manifest(Manifest.FORMAT, s.abbr(), s.fips(), s.name(), s.seats(), "Redistricting Data Hub", List.of(inputFile(tractZip)), layer.memberFiles(), Instant.now().truncatedTo(ChronoUnit.SECONDS).toString(), n, layer.totalPopulation(), m, adj.hashedPairs(), adj.fallbackPairs(), repair.componentsBefore(), bridgeInfo, pinfo, SIMPLIFY_METRES, voteMap, warnings);
        TractCache.write(cacheRoot, s, manifest, tracts, edges, simplified, voteInfo == null ? null : Election.Y2024, dem, rep);
        double secs = (System.nanoTime() - t0) / 1e9;
        return new ImportSummary(s, n, layer.totalPopulation(), m, repair.bridges().size(), repair.componentsBefore(), adj.fallbackPairs(), voteInfo == null ? "-" : "2024", voteInfo == null ? 0 : voteInfo.dem(), voteInfo == null ? 0 : voteInfo.rep(), voteInfo == null ? n : voteInfo.tractsMissing(), voteInfo == null ? 0 : voteInfo.blocksOutside(), secs, warnings);
    }
    
    /**
     * A vote file must have some D or R votes in the state's tracts. Otherwise it is the wrong file
     * (another state's, say), and plans drawn from it would split ties; nothing is cached.
     */
    private static void requireVotes(TractVotes v, String file, StateInfo s) {
        if (v.dem() + v.rep() <= 0) {
            throw new DataException(file + " has no D or R presidential votes in " + s.abbr() + "'s tracts (" + v.blocks() + " blocks read, " + v.blocksOutside() + " outside them). Check that it is the right file for " + s.abbr() + ".");
        }
    }
    
    /**
     * Sums the national 2020 file into every listed state's cache (one pass over the CSV) and
     * reports per state. TOT_POP mismatches with the PL population are warnings, not errors.
     */
    public static Map<String, Manifest.VoteInfo> import2020(Path nationalZip, List<StateInfo> states, Path cacheRoot) {
        Map<String, Set<String>> tractsByFips = new LinkedHashMap<>();
        Map<String, TractCache.Tracts> tables = new HashMap<>();
        for (StateInfo s : states) {
            TractCache.Tracts t = TractCache.readTracts(TractCache.stateDir(cacheRoot, s));
            tables.put(s.fips(), t);
            tractsByFips.put(s.fips(), new HashSet<>(List.of(t.geoid())));
        }
        Map<String, TractVotes> all;
        try (InputStream in = BlockVotes.openCsv(nationalZip)) {
            all = BlockVotes.readNational2020(in, nationalZip.getFileName().toString(), tractsByFips);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + nationalZip, e);
        }
        String sha = Json.sha256(nationalZip);
        Map<String, Manifest.VoteInfo> out = new LinkedHashMap<>();
        for (StateInfo s : states) {
            TractVotes v = all.get(s.fips());
            TractCache.Tracts t = tables.get(s.fips());
            if (v == null || v.blocks() == 0) {
                throw new DataException(nationalZip.getFileName() + " has no blocks for " + s.abbr() + ".");
            }
            requireVotes(v, nationalZip.getFileName().toString(), s);
            long pl = 0;
            for (int p : t.pop()) {
                pl += p;
            }
            String warn = null;
            if (Math.abs(v.totPop() - pl) > 0.0005 * pl) {
                warn = String.format(Locale.ROOT, "TOT_POP in %s sums to %,.0f for %s, %.1f%% of the PL population " + "%,d; population comes from PL, the file is used for votes only.", nationalZip.getFileName(), v.totPop(), s.abbr(), 100.0 * v.totPop() / pl, pl);
            }
            if (v.blocksOutside() > 0) {
                String w = v.blocksOutside() + " blocks for " + s.abbr() + " in " + nationalZip.getFileName() + " fall outside its tracts and were skipped.";
                warn = warn == null ? w : warn + " " + w;
            }
            List<String> ids = List.of(t.geoid());
            var dem = new double[ids.size()];
            var rep = new double[ids.size()];
            fill(v, ids, dem, rep);
            var info = new Manifest.VoteInfo("2020", nationalZip.getFileName().toString(), sha, v.dem(), v.rep(), v.demShare(), v.blocks(), v.blocksOutside(), v.tractsMissing(ids), v.totPop(), warn);
            TractCache.writeVotes(cacheRoot, s, Election.Y2020, dem, rep, info);
            out.put(s.abbr(), info);
        }
        return out;
    }
    
    /**
     * Imports every listed state from an RDH download folder: tract and 2024 files in parallel
     * (structured concurrency, platform threads), then the national 2020 file once. Per-state
     * failures are recorded in the summary rather than stopping the others.
     */
    public static List<ImportResult> importAll(Path rdhDir, List<StateInfo> states, Path cacheRoot, int parallel, Consumer<String> log) throws InterruptedException {
        if (states.isEmpty()) {
            throw new DataException("No states to import. Choose at least one state's RDH files (their names start with the state, like me_pl2020_t.zip).");
        }
        // Each state imports once: two parallel imports of one state would race on its cache folder.
        states = List.copyOf(new LinkedHashSet<>(states));
        List<String> missing = RdhFiles.missing(rdhDir, states);
        if (!missing.isEmpty()) {
            throw new MissingFilesException(rdhDir, missing);
        }
        var permits = new Semaphore(Math.max(1, parallel));
        List<StructuredTaskScope.Subtask<ImportResult>> tasks = new ArrayList<>();
        try (var scope = StructuredTaskScope.open(StructuredTaskScope.Joiner.<ImportResult>allUntil(_ -> false), cfg -> cfg.withThreadFactory(Thread.ofPlatform().name("import-", 0).factory()))) {
            for (StateInfo s : states) {
                tasks.add(scope.fork(() -> {
                    permits.acquire();
                    long t0 = System.nanoTime();
                    try {
                        Path votes = RdhFiles.rdhHas2024(s) ? RdhFiles.votes2024Zip(rdhDir, s) : null;
                        ImportSummary sum = importState(s, RdhFiles.tractZip(rdhDir, s), votes, cacheRoot);
                        log.accept(String.format(Locale.ROOT, "imported %s: %d tracts in %.1f s", s.abbr(), sum.tracts(), sum.seconds()));
                        return sum;
                    } catch (RuntimeException e) {
                        log.accept("FAILED " + s.abbr() + ": " + e.getMessage());
                        return new ImportFailure(s, (System.nanoTime() - t0) / 1e9, e.toString());
                    } finally {
                        permits.release();
                    }
                }));
            }
            scope.join();
        }
        List<ImportResult> results = new ArrayList<>();
        for (var t : tasks) {
            results.add(t.get());
        }
        List<StateInfo> imported = results.stream().filter(ImportResult::ok).map(ImportResult::state).toList();
        if (!imported.isEmpty()) {
            long t0 = System.nanoTime();
            Map<String, Manifest.VoteInfo> v2020 = import2020(RdhFiles.national2020(rdhDir), imported, cacheRoot);
            log.accept(String.format(Locale.ROOT, "summed %s for %d states in %.1f s", RdhFiles.NATIONAL_2020, v2020.size(), (System.nanoTime() - t0) / 1e9));
            for (int i = 0; i < results.size(); i++) {
                if (!(results.get(i) instanceof ImportSummary(
                        var state, var tracts, var population, var edges, var bridges, var components,
                        var fallbackPairs, var election, var dem, var rep, var withoutVotes, var blocksOutside,
                        var seconds, var warnings
                ))) {
                    continue;
                }
                Manifest.VoteInfo v = v2020.get(state.abbr());
                if (election.equals("-") && v != null) {
                    // No 2024 file (California): runs fall back to 2020 by default.
                    List<String> w = new ArrayList<>(warnings);
                    if (v.warning() != null) {
                        w.add(v.warning());
                    }
                    results.set(i, new ImportSummary(state, tracts, population, edges, bridges, components, fallbackPairs, "2020 (fallback)", v.dem(), v.rep(), v.tractsMissing(), v.blocksOutside(), seconds, w));
                }
                else if (v != null && v.warning() != null) {
                    // 2020 file problems (TOT_POP mismatches) are warnings for every state.
                    List<String> w = new ArrayList<>(warnings);
                    w.add("2020 votes: " + v.warning());
                    results.set(i, new ImportSummary(state, tracts, population, edges, bridges, components, fallbackPairs, election, dem, rep, withoutVotes, blocksOutside, seconds, w));
                }
            }
        }
        return results;
    }
    
    private static void fill(TractVotes v, List<String> geoids, double[] dem, double[] rep) {
        for (int i = 0; i < geoids.size(); i++) {
            double[] t = v.byTract().get(geoids.get(i));
            if (t != null) {
                dem[i] = t[0];
                rep[i] = t[1];
            }
        }
    }
    
    static List<Geometry> simplify(List<Geometry> projected) {
        Geometry[] arr = projected.toArray(Geometry[]::new);
        try {
            Geometry[] s = CoverageSimplifier.simplify(arr, SIMPLIFY_METRES);
            List<Geometry> out = new ArrayList<>(s.length);
            for (int i = 0; i < s.length; i++) {
                out.add(s[i] == null || s[i].isEmpty() ? arr[i] : s[i]);
            }
            return out;
        } catch (RuntimeException e) {
            List<Geometry> out = new ArrayList<>(arr.length);
            for (Geometry g : arr) {
                out.add(TopologyPreservingSimplifier.simplify(g, SIMPLIFY_METRES));
            }
            return out;
        }
    }
    
    private static Manifest.InputFile inputFile(Path p) {
        try {
            return new Manifest.InputFile(p.getFileName().toString(), Json.sha256(p), Files.size(p));
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + p, e);
        }
    }
    
    private static RdhTractImporter.TractLayer single(List<RdhTractImporter.TractLayer> layers, StateInfo s) {
        if (layers.size() != 1) {
            throw new DataException("Expected one tract layer for " + s.abbr() + ", found " + layers.size() + ".");
        }
        return layers.getFirst();
    }
    
    /**
     * How one state's import ended: its summary, or why it failed.
     */
    public sealed interface ImportResult permits ImportSummary, ImportFailure {
        
        StateInfo state();
        
        default boolean ok() {
            return this instanceof ImportSummary;
        }
    }
    
    /**
     * A state that imported: one line of the import summary table.
     */
    public record ImportSummary(StateInfo state, int tracts, long population, int edges, int bridges, int componentsBefore, int fallbackPairs, String defaultElection, double dem, double rep, long tractsWithoutVotes, long blocksOutside, double seconds, List<String> warnings) implements ImportResult {
        
        public double demShare() {
            return dem + rep > 0 ? dem / (dem + rep) : 0.5;
        }
    }
    
    /**
     * A state whose import failed after {@code seconds}, with the error.
     */
    public record ImportFailure(StateInfo state, double seconds, String error) implements ImportResult {
    }
    
    /**
     * Raised when RDH files are missing; lists exactly which to download.
     */
    public static final class MissingFilesException extends DataException {
        private final List<String> files;
        
        public MissingFilesException(Path dir, List<String> files) {
            super("Missing " + files.size() + " RDH file(s) in " + dir + ": " + String.join(", ", files));
            this.files = List.copyOf(files);
        }
        
        public List<String> files() {
            return files;
        }
    }
}
