package autoredistrict.cli;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;

import autoredistrict.data.DataException;
import autoredistrict.data.StateImporter;
import autoredistrict.data.StateInfo;
import autoredistrict.data.States;
import autoredistrict.service.ImportService;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** The autoredistrict command line. Commands are thin: all work happens in the service module. */
@Command(name = "autoredistrict", mixinStandardHelpOptions = true, version = "autoredistrict 2.0",
        description = "Seat-targeted congressional maps from Redistricting Data Hub tract data.",
        subcommands = {Main.ImportCommand.class})
public final class Main implements Runnable {

    @Option(names = "--cache-dir", description = "Cache folder (default: .cache).", defaultValue = ".cache",
            scope = CommandLine.ScopeType.INHERIT)
    Path cacheDir;

    public static void main(String[] args) {
        System.exit(run(args));
    }

    public static int run(String... args) {
        CommandLine cl = new CommandLine(new Main());
        cl.setExecutionExceptionHandler((ex, cmd, parsed) -> {
            if (ex instanceof DataException) {
                cmd.getErr().println("error: " + ex.getMessage());
                return 2;
            }
            throw ex;
        });
        return cl.execute(args);
    }

    @Override
    public void run() {
        new CommandLine(this).usage(System.out);
    }

    static List<StateInfo> parseStates(String spec) {
        if (spec == null || spec.equalsIgnoreCase("all")) {
            return States.all();
        }
        List<StateInfo> out = new ArrayList<>();
        for (String s : spec.split(",")) {
            if (!s.isBlank()) {
                out.add(States.require(s));
            }
        }
        return out;
    }

    @Command(name = "import", mixinStandardHelpOptions = true,
            description = "Import RDH tract zips and vote files from a download folder into the cache.")
    static final class ImportCommand implements Callable<Integer> {

        @CommandLine.ParentCommand
        Main parent;

        @Option(names = "--rdh-dir", required = true, description = "Folder with the RDH downloads.")
        Path rdhDir;

        @Option(names = "--states", defaultValue = "all", description = "Comma-separated states, or all (default).")
        String states;

        @Option(names = "--parallel", defaultValue = "6", description = "States imported at once (default 6).")
        int parallel;

        @Override
        public Integer call() throws Exception {
            List<StateInfo> list = parseStates(states);
            long t0 = System.nanoTime();
            List<StateImporter.ImportSummary> rows;
            try {
                rows = ImportService.importFolder(rdhDir, list, parent.cacheDir, parallel, System.out::println);
            } catch (StateImporter.MissingFilesException e) {
                System.err.println("Missing RDH files in " + rdhDir + ". Download these from redistrictingdatahub.org:");
                e.files().forEach(f -> System.err.println("  " + f));
                return 3;
            }
            System.out.println();
            System.out.print(ImportService.summaryTable(rows));
            rows.stream().filter(r -> !r.warnings().isEmpty()).forEach(r ->
                    r.warnings().forEach(w -> System.out.println("warning " + r.state().abbr() + ": " + w)));
            System.out.printf(Locale.ROOT, "%nImported %d of %d states in %.1f s.%n",
                    rows.stream().filter(StateImporter.ImportSummary::ok).count(), rows.size(),
                    (System.nanoTime() - t0) / 1e9);
            return rows.stream().allMatch(StateImporter.ImportSummary::ok) ? 0 : 1;
        }
    }
}
