package autoredistrict.data;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Presidential votes from RDH block files, summed to tracts by the first 11 characters of the
 * block GEOID. RDH splits each precinct's votes among its blocks by voting-age population, so
 * votes are fractional doubles. Population never comes from these files.
 *
 * <ul>
 *   <li>2024: {@code xx_2024_gen_2020_blocks_csv.zip}, one per state. Only the two major-party
 *       presidential columns are read: {@code G24PREDHAR}/{@code G24PRERTRU}, or Wisconsin's
 *       {@code PREDEM24}/{@code PREREP24}. Minor {@code G24PRED*} columns (Colorado's
 *       {@code G24PREDPER}) are ignored.</li>
 *   <li>2020: {@code national_block_2020_pres_results.zip}, all states in one CSV:
 *       {@code G20PREDBID}/{@code G20PRERTRU}, with {@code STATEAB} and {@code TOT_POP}.</li>
 * </ul>
 */
public final class BlockVotes {
    
    private BlockVotes() {
    }
    
    /**
     * Reads a 2024 per-state block CSV. Blocks outside {@code tracts} are counted, not summed.
     */
    public static TractVotes read2024(InputStream csv, String source, String stateFips, Set<String> tracts) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(csv, StandardCharsets.UTF_8), 1 << 16)) {
            List<String> header = Csv.header(reader, source);
            int geoid = Csv.indexOf(header, "GEOID20");
            int dem = Csv.indexOf(header, "G24PREDHAR");
            int rep = Csv.indexOf(header, "G24PRERTRU");
            if (dem < 0 || rep < 0) {
                dem = Csv.indexOf(header, "PREDEM24");
                rep = Csv.indexOf(header, "PREREP24");
            }
            if (geoid < 0) {
                throw new DataException(source + " has no GEOID20 column.");
            }
            if (dem < 0 || rep < 0) {
                throw new DataException(source + " has neither G24PREDHAR/G24PRERTRU nor PREDEM24/PREREP24 " + "presidential columns.");
            }
            return sum(reader, source, Election.Y2024, geoid, dem, rep, -1, stateFips, null, tracts).get(stateFips);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + source, e);
        }
    }
    
    /**
     * Reads the 2020 national block file once for several states.
     *
     * @param tractsByFips the tract GEOIDs of each state to keep, by state FIPS
     */
    public static Map<String, TractVotes> readNational2020(InputStream csv, String source, Map<String, Set<String>> tractsByFips) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(csv, StandardCharsets.UTF_8), 1 << 16)) {
            List<String> header = Csv.header(reader, source);
            int geoid = Csv.indexOf(header, "GEOID20");
            int dem = Csv.indexOf(header, "G20PREDBID");
            int rep = Csv.indexOf(header, "G20PRERTRU");
            int totPop = Csv.indexOf(header, "TOT_POP");
            if (geoid < 0 || dem < 0 || rep < 0) {
                throw new DataException(source + " lacks GEOID20, G20PREDBID or G20PRERTRU.");
            }
            return sum(reader, source, Election.Y2020, geoid, dem, rep, totPop, null, tractsByFips, null);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + source, e);
        }
    }
    
    private static Map<String, TractVotes> sum(BufferedReader reader, String source, Election election, int geoidCol, int demCol, int repCol, int popCol, String onlyFips, Map<String, Set<String>> tractsByFips, Set<String> onlyTracts) throws IOException {
        int[] cols = popCol >= 0 ? new int[]{geoidCol, demCol, repCol, popCol} : new int[]{geoidCol, demCol, repCol};
        int[] order = cols.clone();
        java.util.Arrays.sort(order);
        int[] pos = new int[cols.length];
        for (int i = 0; i < cols.length; i++) {
            pos[i] = java.util.Arrays.binarySearch(order, cols[i]);
        }
        String[] picked = new String[cols.length];
        Map<String, Acc> accs = new HashMap<>();
        String line;
        long row = 1;
        while ((line = reader.readLine()) != null) {
            row++;
            if (line.isEmpty()) {
                continue;
            }
            if (!Csv.pick(line, order, picked)) {
                throw new DataException(source + " row " + row + " has too few columns.");
            }
            String block = GeoIds.block(picked[pos[0]], source);
            String fips = block.substring(0, 2);
            Set<String> tracts;
            if (onlyFips != null) {
                tracts = onlyTracts;
            }
            else {
                tracts = tractsByFips.get(fips);
                if (tracts == null) {
                    continue; // a state we aren't importing (or DC)
                }
            }
            Acc acc = accs.computeIfAbsent(fips, f -> new Acc());
            acc.blocks++;
            double d = Csv.number(picked[pos[1]], "the D column", source);
            double r = Csv.number(picked[pos[2]], "the R column", source);
            if (popCol >= 0) {
                acc.totPop += Csv.number(picked[pos[3]], "TOT_POP", source);
            }
            String tract = block.substring(0, GeoIds.TRACT_WIDTH);
            if ((onlyFips != null && !fips.equals(onlyFips)) || !tracts.contains(tract)) {
                acc.outside++;
                continue;
            }
            double[] t = acc.byTract.computeIfAbsent(tract, k -> new double[2]);
            t[0] += d;
            t[1] += r;
            acc.dem += d;
            acc.rep += r;
        }
        Map<String, TractVotes> out = new HashMap<>();
        if (onlyFips != null) {
            Acc acc = accs.getOrDefault(onlyFips, new Acc());
            for (Map.Entry<String, Acc> e : accs.entrySet()) {
                if (!e.getKey().equals(onlyFips)) {
                    acc.blocks += e.getValue().blocks;
                    acc.outside += e.getValue().blocks;
                }
            }
            out.put(onlyFips, acc.result(election, source));
        }
        else {
            for (String fips : tractsByFips.keySet()) {
                out.put(fips, accs.getOrDefault(fips, new Acc()).result(election, source));
            }
        }
        return out;
    }
    
    /**
     * Opens the CSV inside an RDH vote zip.
     */
    public static InputStream openCsv(Path zip) {
        try {
            return RdhTractImporter.openZipMember(zip, ".csv");
        } catch (IOException e) {
            throw new UncheckedIOException("Could not open " + zip, e);
        }
    }
    
    /**
     * Which presidential race.
     */
    public enum Election {
        Y2024("2024"), Y2020("2020");
        
        private final String label;
        
        Election(String label) {
            this.label = label;
        }
        
        public static Election parse(String s) {
            return switch (s.strip()) {
                case "2024" -> Y2024;
                case "2020" -> Y2020;
                default -> throw new DataException("Unknown election '" + s + "'. Use 2024 or 2020.");
            };
        }
        
        public String label() {
            return label;
        }
    }
    
    /**
     * D and R votes per tract GEOID for one state and election, with coverage counts.
     */
    public record TractVotes(Election election, String source, Map<String, double[]> byTract, double dem, double rep, long blocks, long blocksOutside, double totPop) {
        
        public double demShare() {
            double t = dem + rep;
            return t > 0 ? dem / t : 0.5;
        }
        
        /**
         * Tracts in {@code tracts} with no block in the vote file.
         */
        public long tractsMissing(List<String> tracts) {
            return tracts.stream().filter(t -> !byTract.containsKey(t)).count();
        }
    }
    
    private static final class Acc {
        final Map<String, double[]> byTract = new HashMap<>();
        double dem;
        double rep;
        long blocks;
        long outside;
        double totPop;
        
        TractVotes result(Election e, String source) {
            return new TractVotes(e, source, byTract, dem, rep, blocks, outside, totPop);
        }
    }
}
