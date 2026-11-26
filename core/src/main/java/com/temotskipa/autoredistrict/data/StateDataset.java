package com.temotskipa.autoredistrict.data;

import com.temotskipa.autoredistrict.data.BlockVotes.Election;
import com.temotskipa.autoredistrict.engine.Graph;
import org.locationtech.jts.geom.Geometry;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * One imported state, read from the cache for one election: tracts, adjacency, votes, and the
 * simplified geometry (loaded on first use). If the requested election isn't cached and 2020 is,
 * the 2020 votes are used with a warning; that is California's case for 2024, since RDH has no
 * 2024 block file for it. A state with no votes at all is an error: seat targets need votes.
 */
@SuppressWarnings("preview") // LazyConstant is a preview API, used on purpose
public value class StateDataset {
    
    private final StateInfo state;
    private final Path dir;
    private final Manifest manifest;
    private final TractCache.Tracts tracts;
    private final TractCache.Edges edges;
    private final double[] dem;
    private final double[] rep;
    private final Election requested;
    private final Election used;
    private final List<String> warnings;
    private final LazyConstant<List<Geometry>> geometry;
    private final LazyConstant<Graph> graph;
    private final LazyConstant<AlbersProjection> projection;
    
    private StateDataset(StateInfo state, Path dir, Manifest manifest, TractCache.Tracts tracts, TractCache.Edges edges, double[] dem, double[] rep, Election requested, Election used, List<String> warnings) {
        this.state = state;
        this.dir = dir;
        this.manifest = manifest;
        this.tracts = tracts;
        this.edges = edges;
        this.dem = dem;
        this.rep = rep;
        this.requested = requested;
        this.used = used;
        this.warnings = List.copyOf(warnings);
        this.geometry = LazyConstant.of(() -> TractCache.readGeometry(dir));
        // A value class can't hand out `this` while it is being built, so the graph is built from
        // the constructor's arguments.
        this.graph = LazyConstant.of(() -> buildGraph(tracts, edges, dem, rep));
        this.projection = LazyConstant.of(() -> manifest.projection().toProjection());
    }
    
    public static StateDataset load(Path cacheRoot, StateInfo state, Election election) {
        Manifest manifest = TractCache.readManifest(cacheRoot, state);
        Path dir = TractCache.stateDir(cacheRoot, state);
        TractCache.Tracts tracts = TractCache.readTracts(dir);
        TractCache.Edges edges = TractCache.readEdges(dir);
        List<String> warnings = new ArrayList<>();
        Election used = election;
        double[][] votes = TractCache.readVotes(dir, election, tracts.geoid());
        if (votes == null && election == Election.Y2024) {
            votes = TractCache.readVotes(dir, Election.Y2020, tracts.geoid());
            if (votes != null) {
                used = Election.Y2020;
                warnings.add(state.abbr() + " has no 2024 block-level results at RDH; using the 2020 presidential " + "results (national_block_2020_pres_results.zip) instead.");
            }
        }
        if (votes == null) {
            throw new DataException("No " + election.label() + " vote data for " + state.abbr() + ". Seat targets need votes: import " + (election == Election.Y2024 ? RdhFiles.votes2024Zip(Path.of("."), state).getFileName() : RdhFiles.NATIONAL_2020) + " with `autoredistrict import`.");
        }
        Manifest.VoteInfo info = manifest.votes() == null ? null : manifest.votes().get(used.label());
        if (info != null && info.warning() != null) {
            warnings.add(info.warning());
        }
        return new StateDataset(state, dir, manifest, tracts, edges, votes[0], votes[1], election, used, warnings);
    }
    
    private static Graph buildGraph(TractCache.Tracts tracts, TractCache.Edges edges, double[] dem, double[] rep) {
        return Graph.of(tracts.pop(), dem, rep, tracts.x(), tracts.y(), tracts.area(), edges.a(), edges.b(), edges.length());
    }
    
    public StateInfo state() {
        return state;
    }
    
    public Manifest manifest() {
        return manifest;
    }
    
    public Graph graph() {
        return graph.get();
    }
    
    public int size() {
        return tracts.size();
    }
    
    public String geoid(int i) {
        return tracts.geoid()[i];
    }
    
    public String[] geoids() {
        return tracts.geoid().clone();
    }
    
    public TractCache.Tracts tracts() {
        return tracts;
    }
    
    public TractCache.Edges edges() {
        return edges;
    }
    
    public long totalPopulation() {
        return manifest.population();
    }
    
    public double totalDem() {
        double s = 0;
        for (double d : dem) {
            s += d;
        }
        return s;
    }
    
    public double totalRep() {
        double s = 0;
        for (double r : rep) {
            s += r;
        }
        return s;
    }
    
    /**
     * Statewide D share of the two-party vote.
     */
    public double demShare() {
        double d = totalDem();
        double r = totalRep();
        return d + r > 0 ? d / (d + r) : 0.5;
    }
    
    public Election usedElection() {
        return used;
    }
    
    /**
     * "2024", or "2020 (fallback)" when the requested election wasn't available.
     */
    public String voteDataLabel() {
        return used == requested ? used.label() : used.label() + " (fallback)";
    }
    
    public List<String> warnings() {
        return warnings;
    }
    
    public List<Geometry> geometry() {
        return geometry.get();
    }
    
    /**
     * Reads the geometry and builds the graph now rather than on first use, for callers that load
     * off the UI thread.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored") // called for the loading, not the value
    public void preload() {
        geometry.get();
        graph.get();
    }
    
    public AlbersProjection projection() {
        return projection.get();
    }
    
    public Path dir() {
        return dir;
    }
}
