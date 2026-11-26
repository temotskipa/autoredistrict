package com.temotskipa.autoredistrict.cli;

import com.temotskipa.autoredistrict.data.DataException;
import com.temotskipa.autoredistrict.data.StateInfo;
import com.temotskipa.autoredistrict.data.States;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
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
    
    @Test
    void republicanSeatsLeaveTheRestToDemocrats() {
        StateInfo nc = States.require("NC");
        assertEquals(10, seats("NC", "--rep-seats", "4").demSeats(nc));
        assertEquals(0, seats("NC", "--rep-seats", "14").demSeats(nc));
        assertEquals(3, seats("NC", "--dem-seats", "3").demSeats(nc));
        DataException e = assertThrows(DataException.class, () -> seats("NC", "--rep-seats", "15").demSeats(nc));
        assertTrue(e.getMessage().startsWith("--rep-seats for NC must be between 0 and 14"), e.getMessage());
        // Asking for both is a usage error (exit 2).
        assertEquals(2, Main.run("--cache-dir", tmp.resolve("cache").toString(), "run", "NC", "--dem-seats", "3", "--rep-seats", "4"));
    }
    
    @Test
    void atLargeStatesAreRefused() {
        assertEquals(44, Main.parseStates("all").size(), "all is every state with districts");
        assertTrue(Main.parseStates("all").stream().noneMatch(StateInfo::atLarge));
        String atLarge = "VT elects its one representative at large, so it has no districts to draw.";
        assertEquals(atLarge, assertThrows(DataException.class, () -> Main.parseStates("ME,VT")).getMessage());
        // run and seat-range refuse them before looking in the cache: a usage error (exit 2).
        String cache = tmp.resolve("cache").toString();
        for (String command : new String[]{"run", "seat-range"}) {
            var err = new ByteArrayOutputStream();
            PrintStream old = System.err;
            System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
            try {
                assertEquals(2, Main.run("--cache-dir", cache, command, "VT"), command);
            } finally {
                System.setErr(old);
            }
            assertEquals("error: " + atLarge, err.toString(StandardCharsets.UTF_8).strip(), command);
        }
    }
    
    private static Main.SeatRequest seats(String... args) {
        var run = new Main.RunCommand();
        new CommandLine(run).parseArgs(args);
        return run.seats;
    }
}
