package autoredistrict.data;

import autoredistrict.data.BlockVotes.Election;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ported from the Python tests/test_rdh_import.py, on RDH-shaped synthetic fixtures.
 */
class RdhTractImporterTest {
    
    @TempDir
    Path tmp;
    
    @Test
    void importsShapefileZipIntoCacheReadableWithoutNetwork() throws Exception {
        List<Fixtures.Tract> tracts = Fixtures.grid("23", 4);
        Path zip = Fixtures.tractZip(tmp, "me_pl2020_t", tracts);
        Path votes = Fixtures.zipResource("/votes/me_2024_standard.csv", tmp.resolve("me_2024_gen_2020_blocks_csv.zip"), "me_2024_gen_2020_blocks.csv");
        Path cache = tmp.resolve("cache");
        StateInfo me = States.require("ME");
        
        StateImporter.ImportSummary sum = StateImporter.importState(me, zip, votes, cache);
        
        assertEquals(16, sum.tracts());
        assertEquals(tracts.stream().mapToLong(Fixtures.Tract::pop).sum(), sum.population());
        Manifest m = TractCache.readManifest(cache, me);
        assertEquals(List.of("me_pl2020_t.shp"), m.memberFiles());
        assertEquals("me_pl2020_t.zip", m.inputFiles().getFirst().name());
        assertEquals(64, m.inputFiles().getFirst().sha256().length());
        assertEquals(24, m.edges(), "4x4 rook grid has 24 edges");
        assertEquals(0, m.bridges().size());
        
        StateDataset ds = StateDataset.load(cache, me, Election.Y2024);
        assertEquals(16, ds.size());
        assertEquals(sum.population(), ds.graph().totalPop());
        assertEquals("23001000100", ds.geoid(0));
        assertEquals(16, ds.geometry().size());
        assertEquals("2024", ds.voteDataLabel());
    }
    
    @Test
    void csvWithNumericGeoidsAndSeparateGeometryIsPadded() throws Exception {
        List<Fixtures.Tract> tracts = Fixtures.grid("01", 4);
        StringBuilder csv = new StringBuilder("GEOID20,NAME,P0010001\n");
        for (Fixtures.Tract t : tracts) {
            // A numeric read lost the leading zero and added ".0".
            csv.append(Long.parseLong(t.geoid())).append(".0,").append(t.name()).append(',').append(t.pop()).append('\n');
        }
        Path counts = tmp.resolve("al_pl2020_t.csv");
        Files.writeString(counts, csv);
        Path bounds = tmp.resolve("al_t_2020_bound");
        Fixtures.shapefile(bounds, "al_t_2020_bound", tracts, false);
        
        List<RdhTractImporter.TractLayer> layers = RdhTractImporter.read(List.of(counts, bounds), null, null);
        
        assertEquals(1, layers.size());
        RdhTractImporter.TractLayer layer = layers.getFirst();
        assertEquals("01", layer.stateFips());
        assertTrue(layer.geoids().stream().allMatch(g -> g.startsWith("01001") && g.length() == 11));
        assertEquals(16, layer.geometries().size());
    }
    
    @Test
    void rejectsFilesForAnotherState() throws Exception {
        Path zip = Fixtures.tractZip(tmp, "me_pl2020_t", Fixtures.grid("23", 4));
        DataException e = assertThrows(DataException.class, () -> RdhTractImporter.read(List.of(zip), "50", null));
        assertTrue(e.getMessage().contains("state FIPS 23, not 50"), e.getMessage());
    }
    
    @Test
    void needsGeometryAndWritesNoCacheWithoutIt() throws Exception {
        StringBuilder csv = new StringBuilder("GEOID20,NAME,P0010001\n");
        for (Fixtures.Tract t : Fixtures.grid("23", 4)) {
            csv.append(t.geoid()).append(',').append(t.name()).append(',').append(t.pop()).append('\n');
        }
        Path counts = tmp.resolve("me_pl2020_t.csv");
        Files.writeString(counts, csv);
        Path cache = tmp.resolve("cache");
        
        DataException e = assertThrows(DataException.class, () -> StateImporter.importState(States.require("ME"), counts, null, cache));
        assertTrue(e.getMessage().contains("No geometry"), e.getMessage());
        assertFalse(TractCache.isImported(cache, States.require("ME")));
    }
    
    @Test
    void needsPopulationCounts() throws Exception {
        Path folder = tmp.resolve("bound");
        Fixtures.shapefile(folder, "me_bound", Fixtures.grid("23", 2), false);
        DataException e = assertThrows(DataException.class, () -> RdhTractImporter.read(List.of(folder), null, null));
        assertTrue(e.getMessage().contains("P0010001"), e.getMessage());
    }
    
    @Test
    void rejectsDuplicateGeoidsAndSeveralStates() throws Exception {
        Path dup = tmp.resolve("dup.csv");
        Files.writeString(dup, "GEOID20,P0010001\n23001000100,5\n23001000100,6\n");
        assertTrue(assertThrows(DataException.class, () -> RdhTractImporter.read(List.of(dup), null, null)).getMessage().contains("duplicate GEOIDs"));
        Path two = tmp.resolve("two.csv");
        Files.writeString(two, "GEOID20,P0010001\n23001000100,5\n50001000100,6\n");
        assertTrue(assertThrows(DataException.class, () -> RdhTractImporter.read(List.of(two), null, null)).getMessage().contains("covers several states (23, 50)"));
    }
    
    @Test
    void rejectsBlockLevelFilesAndBadZips() throws Exception {
        Path blocks = tmp.resolve("me_pl2020_b.csv");
        Files.writeString(blocks, "GEOID20,P0010001\n230010001001000,5\n");
        assertTrue(assertThrows(DataException.class, () -> RdhTractImporter.read(List.of(blocks), null, null)).getMessage().contains("block level"));
        Path bad = tmp.resolve("bad.zip");
        Files.writeString(bad, "not a zip");
        assertTrue(assertThrows(DataException.class, () -> RdhTractImporter.read(List.of(bad), null, null)).getMessage().contains("not a valid zip"));
        assertTrue(assertThrows(DataException.class, () -> RdhTractImporter.read(List.of(tmp.resolve("nope.zip")), null, null)).getMessage().contains("not found"));
    }
    
    @Test
    void missingFilesAreListedByName() throws Exception {
        Path dir = Files.createDirectories(tmp.resolve("rdh"));
        Files.writeString(dir.resolve("me_pl2020_t.zip"), "x");
        List<String> missing = RdhFiles.missing(dir, List.of(States.require("ME"), States.require("CA")));
        assertEquals(List.of("me_2024_gen_2020_blocks_csv.zip", "ca_pl2020_t.zip", RdhFiles.NATIONAL_2020), missing);
    }
}
