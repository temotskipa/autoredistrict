package com.temotskipa.autoredistrict.cli;

import com.temotskipa.autoredistrict.data.DataException;
import com.temotskipa.autoredistrict.data.States;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MainTest {
    
    @TempDir
    Path tmp;
    
    @Test
    void aStateListThatNamesNoStateIsRefused() {
        for (String spec : new String[]{"", " ", ",,", " , "}) {
            DataException e = assertThrows(DataException.class, () -> Main.parseStates(spec), "'" + spec + "'");
            assertTrue(e.getMessage().startsWith("--states names no state."), e.getMessage());
        }
        assertEquals(List.of(States.require("ME"), States.require("NC")), Main.parseStates("ME, nc,23"));
        // An import of nothing is a usage error (exit 2), not a success.
        assertEquals(2, Main.run("--cache-dir", tmp.resolve("cache").toString(), "import", "--rdh-dir", tmp.toString(), "--states", ",,"));
    }
}
