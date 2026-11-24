package com.temotskipa.autoredistrict.data;

import com.temotskipa.autoredistrict.data.BlockVotes.Election;
import com.temotskipa.autoredistrict.data.BlockVotes.TractVotes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class BlockVotesTest {
    
    @TempDir
    Path tmp;
    
    @Test
    void standard2024ColumnsIgnoreMinorDemocraticLine() {
        TractVotes v = BlockVotes.read2024(Fixtures.resource("/votes/me_2024_standard.csv"), "me.csv", "23", Set.of("23001000100", "23001000200", "23001000300"));
        assertArrayEquals(new double[]{12.0, 5.0}, v.byTract().get("23001000100"), 1e-12);
        assertArrayEquals(new double[]{3.0, 7.0}, v.byTract().get("23001000200"), 1e-12);
        // Only G24PREDPER has votes in tract 300; it is not a major-party column.
        assertArrayEquals(new double[]{0.0, 0.0}, v.byTract().get("23001000300"), 1e-12);
        assertEquals(15.0, v.dem(), 1e-12);
        assertEquals(12.0, v.rep(), 1e-12);
        assertEquals(4, v.blocks());
        assertEquals(0, v.blocksOutside());
    }
    
    @Test
    void wisconsinUsesPredem24AndPrerep24() {
        TractVotes v = BlockVotes.read2024(Fixtures.resource("/votes/wi_2024.csv"), "wi.csv", "55", Set.of("55001000100", "55001000200"));
        assertArrayEquals(new double[]{3.0, 4.0}, v.byTract().get("55001000100"), 1e-12);
        assertArrayEquals(new double[]{4.0, 4.0}, v.byTract().get("55001000200"), 1e-12);
    }
    
    @Test
    void alabamaFourteenCharacterGeoidsArePadded() {
        TractVotes v = BlockVotes.read2024(Fixtures.resource("/votes/al_2024.csv"), "al.csv", "01", Set.of("01001000100", "01001000200"));
        assertArrayEquals(new double[]{4.0, 6.0}, v.byTract().get("01001000100"), 1e-12);
        assertArrayEquals(new double[]{0.25, 0.75}, v.byTract().get("01001000200"), 1e-12);
        assertEquals(0, v.blocksOutside());
    }
    
    @Test
    void blocksOutsideTheStatesTractsAreCountedNotSummed() {
        TractVotes v = BlockVotes.read2024(Fixtures.resource("/votes/me_2024_standard.csv"), "me.csv", "23", Set.of("23001000100"));
        assertEquals(2, v.blocksOutside());
        assertEquals(12.0, v.dem(), 1e-12);
        assertEquals(1, v.tractsMissing(List.of("23001000100", "23001000900")));
    }
    
    @Test
    void nonFiniteVoteValuesAreAnErrorNamingFileAndColumn() {
        for (String bad : new String[]{"NaN", "Infinity", "-Infinity"}) {
            String csv = "GEOID20,G24PREDHAR,G24PRERTRU\n230010001001000," + bad + ",4.5\n";
            DataException e = assertThrows(DataException.class, () -> BlockVotes.read2024(new java.io.ByteArrayInputStream(csv.getBytes(java.nio.charset.StandardCharsets.UTF_8)), "me.csv", "23", Set.of("23001000100")));
            assertEquals("me.csv has a non-finite value '" + bad + "' in the D column.", e.getMessage());
        }
    }

    @Test
    void fileWithoutPresidentialColumnsIsAnErrorNamingIt() {
        DataException e = assertThrows(DataException.class, () -> BlockVotes.read2024(Fixtures.resource("/votes/no_president.csv"), "xx_2024_gen_2020_blocks.csv", "23", Set.of()));
        assertTrue(e.getMessage().startsWith("xx_2024_gen_2020_blocks.csv has neither"), e.getMessage());
    }
    
    @Test
    void national2020FileSumsSeveralStatesAndSkipsOthers() {
        Map<String, TractVotes> all = BlockVotes.readNational2020(Fixtures.resource("/votes/national_2020.csv"), "nat.csv", Map.of("01", Set.of("01001000100", "01001000200"), "06", Set.of("06001000100", "06001000200", "06001000300")));
        TractVotes al = all.get("01");
        assertArrayEquals(new double[]{2.56, 5.79}, al.byTract().get("01001000100"), 1e-12);
        assertEquals(31.0, al.totPop(), 1e-12);
        TractVotes ca = all.get("06");
        assertEquals(311.5, ca.dem(), 1e-9);
        assertEquals(103.25, ca.rep(), 1e-9);
        // A block with votes and TOT_POP 0 keeps its votes.
        assertArrayEquals(new double[]{4.0, 1.0}, ca.byTract().get("06001000200"), 1e-12);
        assertEquals(2, all.size(), "DC is skipped");
    }
    
    @Test
    void californiaFallsBackTo2020WithAWarning() throws Exception {
        StateInfo ca = States.require("CA");
        Path cache = tmp.resolve("cache");
        List<Fixtures.Tract> tracts = Fixtures.grid("06", 2).subList(0, 3);
        StateImporter.importState(ca, Fixtures.tractZip(tmp, "ca_pl2020_t", tracts), null, cache);
        Path nat = Fixtures.zipResource("/votes/national_2020.csv", tmp.resolve(RdhFiles.NATIONAL_2020), "national_block_2020_pres_results/national_block_2020_pres_results.csv");
        
        Map<String, Manifest.VoteInfo> info = StateImporter.import2020(nat, List.of(ca), cache);
        
        assertTrue(info.get("CA").warning().contains("TOT_POP"), "TOT_POP 520 vs PL population is a warning");
        StateDataset ds = StateDataset.load(cache, ca, Election.Y2024);
        assertEquals(Election.Y2020, ds.usedElection());
        assertEquals("2020 (fallback)", ds.voteDataLabel());
        assertTrue(ds.warnings().getFirst().contains("no 2024 block-level results"));
        assertEquals(311.5, ds.totalDem(), 1e-9);
        assertEquals(StateDataset.load(cache, ca, Election.Y2020).usedElection(), Election.Y2020);
    }
    
    @Test
    void stateWithNoVotesStopsTheRun() throws Exception {
        StateInfo me = States.require("ME");
        Path cache = tmp.resolve("cache");
        StateImporter.importState(me, Fixtures.tractZip(tmp, "me_pl2020_t", Fixtures.grid("23", 2)), null, cache);
        DataException e = assertThrows(DataException.class, () -> StateDataset.load(cache, me, Election.Y2024));
        assertTrue(e.getMessage().startsWith("No 2024 vote data for ME"), e.getMessage());
    }
}
