package autoredistrict.data;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Where the Redistricting Data Hub downloads are expected, by RDH's own file names:
 * {@code xx_pl2020_t.zip} (2020 PL 94-171 tracts with geometry), {@code xx_2024_gen_2020_blocks_csv.zip}
 * (2024 general election results on 2020 blocks; RDH has none for California), and
 * {@code national_block_2020_pres_results.zip} (2020 presidential results on all blocks).
 */
public final class RdhFiles {
    
    public static final String NATIONAL_2020 = "national_block_2020_pres_results.zip";
    
    private RdhFiles() {
    }
    
    public static Path tractZip(Path dir, StateInfo s) {
        return dir.resolve(s.key() + "_pl2020_t.zip");
    }
    
    public static Path votes2024Zip(Path dir, StateInfo s) {
        return dir.resolve(s.key() + "_2024_gen_2020_blocks_csv.zip");
    }
    
    public static Path national2020(Path dir) {
        return dir.resolve(NATIONAL_2020);
    }
    
    /**
     * RDH publishes 2024 block results for every state but California (precincts only there).
     */
    public static boolean rdhHas2024(StateInfo s) {
        return !s.abbr().equals("CA");
    }
    
    /**
     * File names that must be downloaded before {@code states} can be imported.
     */
    public static List<String> missing(Path dir, List<StateInfo> states) {
        List<String> out = new ArrayList<>();
        for (StateInfo s : states) {
            if (!Files.isRegularFile(tractZip(dir, s))) {
                out.add(tractZip(dir, s).getFileName().toString());
            }
            if (rdhHas2024(s) && !Files.isRegularFile(votes2024Zip(dir, s))) {
                out.add(votes2024Zip(dir, s).getFileName().toString());
            }
        }
        if (!Files.isRegularFile(national2020(dir))) {
            out.add(NATIONAL_2020);
        }
        return out;
    }
}
