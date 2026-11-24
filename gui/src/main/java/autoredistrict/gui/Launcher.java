package autoredistrict.gui;

import javafx.application.Application;

import java.nio.file.Path;

/**
 * GUI entry point. Kept separate from {@link App} (which extends {@link Application}) so JavaFX
 * can start from the class path.
 *
 * <pre>
 *   autoredistrict-gui [--cache-dir .cache]
 *   autoredistrict-gui --snapshot ME --dem-seats 1 --out me.png [--seeds N]   (run, save window PNG, exit)
 *   autoredistrict-gui --exit-after-show                                       (print startup time, exit)
 * </pre>
 */
public final class Launcher {
    
    static Options options = new Options(Path.of(".cache"), null, null, null, null, false);
    
    private Launcher() {
    }
    
    static void main(String[] args) {
        Path cache = Path.of(".cache");
        String snapshot = null;
        Integer dem = null;
        Path out = null;
        Integer seeds = null;
        boolean exit = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--cache-dir" -> cache = Path.of(args[++i]);
                case "--snapshot" -> snapshot = args[++i];
                case "--dem-seats" -> dem = Integer.parseInt(args[++i]);
                case "--out" -> out = Path.of(args[++i]);
                case "--seeds" -> seeds = Integer.parseInt(args[++i]);
                case "--exit-after-show" -> exit = true;
                default -> {
                    System.err.println("unknown option " + args[i]);
                    System.exit(2);
                }
            }
        }
        if (snapshot != null && out == null) {
            out = Path.of(snapshot.toLowerCase() + "-gui.png");
        }
        options = new Options(cache, snapshot, dem, out, seeds, exit);
        Application.launch(App.class, args);
    }
    
    /**
     * Parsed command-line options.
     */
    record Options(Path cacheDir, String snapshotState, Integer demSeats, Path out, Integer seeds, boolean exitAfterShow) {
    }
}
