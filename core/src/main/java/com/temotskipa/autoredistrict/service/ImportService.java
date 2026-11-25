package com.temotskipa.autoredistrict.service;

import com.temotskipa.autoredistrict.data.StateImporter;
import com.temotskipa.autoredistrict.data.StateImporter.ImportFailure;
import com.temotskipa.autoredistrict.data.StateImporter.ImportResult;
import com.temotskipa.autoredistrict.data.StateImporter.ImportSummary;
import com.temotskipa.autoredistrict.data.StateInfo;

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
    
    public static List<ImportResult> importFolder(Path rdhDir, List<StateInfo> states, Path cacheRoot, int parallel, Consumer<String> log) throws InterruptedException {
        return StateImporter.importAll(rdhDir, states, cacheRoot, parallel, log);
    }
    
    /**
     * One line per state that failed to import ("VT: the error"), empty when every state imported.
     */
    public static List<String> failures(List<ImportResult> rows) {
        return rows.stream().<String>mapMulti((r, out) -> {
            if (r instanceof ImportFailure(var state, var _, var error)) {
                out.accept(state.abbr() + ": " + error);
            }
        }).toList();
    }
    
    /**
     * Markdown table with one row per state, plus a totals row. A failed state shows only its time
     * and error.
     */
    public static String summaryTable(List<ImportResult> rows) {
        var sb = new StringBuilder("""
                                   | ST | FIPS | tracts | population | edges | bridges | components | fallback pairs | election \
                                   | D votes | R votes | D share | tracts w/o votes | blocks outside | s | status |
                                   |---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---|
                                   """);
        long tracts = 0;
        long pop = 0;
        double dem = 0;
        double rep = 0;
        for (ImportResult result : rows) {
            switch (result) {
                case ImportSummary r -> {
                    tracts += r.tracts();
                    pop += r.population();
                    dem += r.dem();
                    rep += r.rep();
                    sb.append(String.format(Locale.ROOT, "| %s | %s | %,d | %,d | %,d | %d | %d | %d | %s | %,.0f | %,.0f | %.4f | %d | %d | %.1f | ok |%n", r.state().abbr(), r.state().fips(), r.tracts(), r.population(), r.edges(), r.bridges(), r.componentsBefore(), r.fallbackPairs(), r.defaultElection(), r.dem(), r.rep(), r.demShare(), r.tractsWithoutVotes(), r.blocksOutside(), r.seconds()));
                }
                case ImportFailure(var state, var seconds, var error) ->
                        sb.append(String.format(Locale.ROOT, "| %s | %s | | | | | | | | | | | | | %.1f | FAILED: %s |%n", state.abbr(), state.fips(), seconds, error));
            }
        }
        sb.append(String.format(Locale.ROOT, "| **total** | | %,d | %,d | | | | | | %,.0f | %,.0f | | | | | %d states |%n", tracts, pop, dem, rep, rows.stream().filter(ImportResult::ok).count()));
        return sb.toString();
    }
}
