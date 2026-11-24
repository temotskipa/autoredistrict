package autoredistrict.service;

import autoredistrict.data.StateImporter;
import autoredistrict.data.StateImporter.ImportSummary;
import autoredistrict.data.StateInfo;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

/**
 * Import facade for the front ends: RDH folder to cache, and the summary table.
 */
public final class ImportService {
    
    private ImportService() {
    }
    
    public static List<ImportSummary> importFolder(Path rdhDir, List<StateInfo> states, Path cacheRoot, int parallel, Consumer<String> log) throws InterruptedException {
        return StateImporter.importAll(rdhDir, states, cacheRoot, parallel, log);
    }
    
    /**
     * Markdown table with one row per state, plus a totals row.
     */
    public static String summaryTable(List<ImportSummary> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append("| ST | FIPS | tracts | population | edges | bridges | components | fallback pairs | election " + "| D votes | R votes | D share | tracts w/o votes | blocks outside | s | status |\n");
        sb.append("|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---|\n");
        long tracts = 0;
        long pop = 0;
        double dem = 0;
        double rep = 0;
        for (ImportSummary r : rows) {
            tracts += r.tracts();
            pop += r.population();
            dem += r.dem();
            rep += r.rep();
            sb.append(String.format(Locale.ROOT, "| %s | %s | %,d | %,d | %,d | %d | %d | %d | %s | %,.0f | %,.0f | %.4f | %d | %d | %.1f | %s |%n", r.state().abbr(), r.state().fips(), r.tracts(), r.population(), r.edges(), r.bridges(), r.componentsBefore(), r.fallbackPairs(), r.defaultElection(), r.dem(), r.rep(), r.demShare(), r.tractsWithoutVotes(), r.blocksOutside(), r.seconds(), r.ok() ? "ok" : "FAILED: " + r.error()));
        }
        sb.append(String.format(Locale.ROOT, "| **total** | | %,d | %,d | | | | | | %,.0f | %,.0f | | | | | %d states |%n", tracts, pop, dem, rep, rows.stream().filter(ImportSummary::ok).count()));
        return sb.toString();
    }
}
