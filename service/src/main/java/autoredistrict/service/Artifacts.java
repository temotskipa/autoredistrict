package autoredistrict.service;

import autoredistrict.data.Json;
import autoredistrict.data.StateDataset;
import autoredistrict.render.Exporters;
import autoredistrict.render.MapData;
import autoredistrict.render.MapRenderer;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Writes a plan's map PNG, JSON report, assignment CSV, GeoJSON and shapefile.
 */
public final class Artifacts {
    
    private Artifacts() {
    }
    
    /**
     * Map data with the standard title and subtitle.
     */
    public static MapData mapData(PlanResult r, StateDataset ds) {
        String title = String.format(Locale.ROOT, "%s (%s) — %d district%s", r.state().name(), r.state().abbr(), r.districts(), r.districts() == 1 ? " (at-large)" : "s");
        String subtitle = String.format(Locale.ROOT, "requested %d D%s · won %s · %s · max dev %.2f%% · closest margin %.1f pts · %s votes", r.demRequested(), r.proportionalDefault() ? " (proportional)" : "", MapRenderer.seats(r.metrics().demWon(), r.metrics().repWon()), r.hit() ? "hit" : "miss", r.metrics().maxDeviation() * 100, r.metrics().closestMargin(), r.voteData());
        return MapData.of(title, subtitle, ds.geometry(), r.assignment(), r.districts(), r.metrics().demShares(), populatedExtent(ds), emptyTracts(ds));
    }
    
    /**
     * Tracts with no people.
     */
    public static boolean[] emptyTracts(StateDataset ds) {
        boolean[] e = new boolean[ds.size()];
        for (int i = 0; i < e.length; i++) {
            e[i] = ds.tracts().pop()[i] == 0;
        }
        return e;
    }
    
    /**
     * Extent of the tracts with people in them.
     */
    public static org.locationtech.jts.geom.Envelope populatedExtent(StateDataset ds) {
        org.locationtech.jts.geom.Envelope e = new org.locationtech.jts.geom.Envelope();
        for (int i = 0; i < ds.size(); i++) {
            if (ds.tracts().pop()[i] > 0) {
                e.expandToInclude(ds.geometry().get(i).getEnvelopeInternal());
            }
        }
        return e;
    }
    
    /**
     * PNG size following the state's aspect ratio.
     */
    public static int[] size(MapData data, int width) {
        double aspect = data.bounds().getHeight() / Math.max(1, data.bounds().getWidth());
        int h = (int) Math.round(Math.max(700, Math.min(1900, width * aspect + 110)));
        return new int[]{width, h};
    }
    
    public static BufferedImage png(MapData data, int width) {
        int[] s = size(data, width);
        return MapRenderer.render(data, s[0], s[1]);
    }
    
    /**
     * Writes {stem}.png, {stem}.json and {stem}-assignment.csv; with {@code exports}, also .geojson and .shp.
     */
    public static List<Path> write(PlanResult r, StateDataset ds, Path dir, String stem, boolean exports) {
        List<Path> out = new ArrayList<>();
        MapData data = mapData(r, ds);
        Path png = dir.resolve(stem + ".png");
        MapRenderer.writePng(png(data, 1600), png);
        out.add(png);
        Path json = dir.resolve(stem + ".json");
        writeReport(r, ds, json);
        out.add(json);
        Path csv = dir.resolve(stem + "-assignment.csv");
        Exporters.assignmentCsv(csv, ds.geoids(), r.assignment());
        out.add(csv);
        if (exports) {
            List<Exporters.DistrictProps> props = props(r);
            Path geojson = dir.resolve(stem + ".geojson");
            Exporters.geoJson(geojson, data.districtShapes(), props, g -> ds.projection().toLonLat(g));
            out.add(geojson);
            Path shp = dir.resolve(stem + "-shp").resolve(stem + ".shp");
            Exporters.shapefile(shp, data.districtShapes(), props, ds.projection().wkt());
            out.add(shp);
        }
        return out;
    }
    
    public static List<Exporters.DistrictProps> props(PlanResult r) {
        List<Exporters.DistrictProps> props = new ArrayList<>();
        for (DistrictResult d : r.metrics().rows()) {
            props.add(new Exporters.DistrictProps(d.district(), d.population(), d.deviation() * 100, d.demShare(), d.winner(), d.margin()));
        }
        return props;
    }
    
    public static void writeReport(PlanResult r, StateDataset ds, Path file) {
        PlanMetrics m = r.metrics();
        Report rep = new Report(r.state().abbr(), r.state().fips(), r.districts(), r.demRequested(), r.proportionalDefault(), r.minMargin(), r.voteData(), r.statewideDemShare(), m.demWon(), m.repWon(), r.hit(), r.pass(PlanOptions.DEFAULT_TOLERANCE), m.maxDeviation() * 100, m.allContiguous(), m.assignedOnce(), m.populationMatches(), m.population(), m.importedPopulation(), m.closestMargin(), r.seeds(), r.baseSeed(), r.bestChain(), r.chainsHit(), r.proposals(), r.flipsPerSecond(), r.seconds(), r.schedule(), r.warnings(), m.rows());
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
            try (OutputStream out = Files.newOutputStream(file)) {
                Json.mapper().writeValue(out, rep);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write " + file, e);
        }
    }
    
    /**
     * Text table of districts for the console.
     */
    public static String districtTable(PlanResult r) {
        StringBuilder sb = new StringBuilder();
        sb.append("| district | population | deviation % | D share | winner | margin (pts) | tracts | Polsby-Popper |\n");
        sb.append("|---:|---:|---:|---:|---|---:|---:|---:|\n");
        for (DistrictResult d : r.metrics().rows()) {
            sb.append(String.format(Locale.ROOT, "| %d | %,d | %+.3f | %.4f | %s | %+.2f | %d | %.3f |%n", d.district(), d.population(), d.deviation() * 100, d.demShare(), d.winner(), d.margin(), d.tracts(), d.polsbyPopper()));
        }
        return sb.toString();
    }
    
    /**
     * The JSON report: request, result, checks and per-district rows.
     */
    public record Report(String state, String fips, int districts, int demRequested, boolean proportionalDefault, double minMargin, String voteData, double statewideDemShare, int demWon, int repWon, boolean hit, boolean pass, double maxDeviationPct, boolean allContiguous, boolean assignedOnce, boolean populationMatches, long population, long importedPopulation, double closestMargin, int seeds, long baseSeed, int bestChain, int chainsHit, long proposals, double flipsPerSecond, double seconds, String schedule, List<String> warnings, List<DistrictResult> districtRows) {
    }
}
