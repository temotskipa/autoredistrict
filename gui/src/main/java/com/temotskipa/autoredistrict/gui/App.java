package com.temotskipa.autoredistrict.gui;

import atlantafx.base.theme.PrimerLight;
import com.temotskipa.autoredistrict.data.BlockVotes.Election;
import com.temotskipa.autoredistrict.data.StateDataset;
import com.temotskipa.autoredistrict.data.StateInfo;
import com.temotskipa.autoredistrict.data.States;
import com.temotskipa.autoredistrict.data.TractCache;
import com.temotskipa.autoredistrict.engine.Annealer;
import com.temotskipa.autoredistrict.engine.Progress;
import com.temotskipa.autoredistrict.gui.Launcher.Options;
import com.temotskipa.autoredistrict.render.Exporters;
import com.temotskipa.autoredistrict.render.MapRenderer;
import com.temotskipa.autoredistrict.service.*;
import javafx.animation.PauseTransition;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.concurrent.Task;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.PixelReader;
import javafx.scene.image.WritableImage;
import javafx.scene.layout.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.awt.image.BufferedImage;
import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The desktop window: pick an imported state, ask for a number of Democratic seats, run the
 * anneal in the background with live progress, and inspect the map and the per-district table.
 * All work goes through the service layer.
 */
public final class App extends Application {
    
    private final Options opts = Launcher.options;
    private final PlanService service = new PlanService(opts.cacheDir());
    
    private final ComboBox<StateInfo> stateBox = new ComboBox<>();
    private final Spinner<Integer> demSpinner = new Spinner<>(0, 1, 0);
    private final Label demRange = new Label();
    private final Spinner<Double> marginSpinner = new Spinner<>(0.0, 30.0, PlanOptions.DEFAULT_MARGIN, 0.5);
    private final Spinner<Integer> seedsSpinner = new Spinner<>(1, 64, Annealer.defaultSeeds());
    private final ChoiceBox<String> electionBox = new ChoiceBox<>(FXCollections.observableArrayList("2024", "2020"));
    private final Button runButton = new Button("Run");
    private final Button cancelButton = new Button("Cancel");
    private final Button importButton = new Button("Import RDH files…");
    private final Button pngButton = new Button("PNG");
    private final Button geoJsonButton = new Button("GeoJSON");
    private final Button csvButton = new Button("Assignment CSV");
    private final Label phaseLabel = new Label("-");
    private final Label flipsLabel = new Label("-");
    private final Label scoreLabel = new Label("-");
    private final Label seatsLabel = new Label("-");
    private final Label statusLabel = new Label();
    private final Label resultLabel = new Label();
    private final ProgressBar progressBar = new ProgressBar(0);
    private final MapView mapView = new MapView();
    private final TableView<DistrictResult> table = new TableView<>();
    
    private Stage stage;
    private StateDataset dataset;
    private PlanResult result;
    /**
     * The dataset {@link #result} was drawn from; exports use it, not whatever is selected now.
     */
    private StateDataset resultDataset;
    private Task<?> running;
    /**
     * The newest state load; completions of older ones are ignored.
     */
    private Task<StateDataset> loading;
    
    private static <T> TableColumn<DistrictResult, T> column(String name, java.util.function.Function<DistrictResult, T> f) {
        TableColumn<DistrictResult, T> c = new TableColumn<>(name);
        c.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(f.apply(cd.getValue())));
        c.setSortable(false);
        return c;
    }
    
    @Override
    public void start(Stage stage) {
        this.stage = stage;
        Application.setUserAgentStylesheet(new PrimerLight().getUserAgentStylesheet());
        BorderPane root = new BorderPane();
        root.setLeft(controls());
        SplitPane split = new SplitPane(mapView, resultsPane());
        split.setOrientation(javafx.geometry.Orientation.VERTICAL);
        split.setDividerPositions(0.68);
        root.setCenter(split);
        Scene scene = new Scene(root, 1360, 900);
        stage.setTitle("autoredistrict — seat-targeted congressional maps");
        stage.setScene(scene);
        stage.setOnShown(e -> {
            long ms = Duration.between(ProcessHandle.current().info().startInstant().orElse(Instant.now()), Instant.now()).toMillis();
            System.out.printf(Locale.ROOT, "first window shown %d ms after JVM start%n", ms);
            if (opts.exitAfterShow()) {
                Platform.runLater(Platform::exit);
            }
            else if (opts.snapshotState() != null) {
                startSnapshot();
            }
        });
        refreshStates();
        stage.show();
    }
    
    private VBox controls() {
        stateBox.setPromptText("Imported states");
        stateBox.setMaxWidth(Double.MAX_VALUE);
        stateBox.setCellFactory(l -> new StateCell());
        stateBox.setButtonCell(new StateCell());
        stateBox.valueProperty().addListener((o, a, s) -> {
            if (s != null) {
                loadState(s);
            }
        });
        electionBox.setValue("2024");
        electionBox.valueProperty().addListener((o, a, b) -> {
            if (stateBox.getValue() != null) {
                loadState(stateBox.getValue());
            }
        });
        for (Spinner<?> s : List.of(demSpinner, marginSpinner, seedsSpinner)) {
            s.setEditable(true);
            s.setMaxWidth(Double.MAX_VALUE);
        }
        GridPane form = new GridPane();
        form.setHgap(8);
        form.setVgap(8);
        javafx.scene.layout.ColumnConstraints labels = new javafx.scene.layout.ColumnConstraints();
        labels.setMinWidth(javafx.scene.layout.Region.USE_PREF_SIZE);
        javafx.scene.layout.ColumnConstraints fields = new javafx.scene.layout.ColumnConstraints();
        fields.setHgrow(Priority.ALWAYS);
        form.getColumnConstraints().addAll(labels, fields);
        demRange.setWrapText(true);
        int r = 0;
        form.addRow(r++, new Label("State"), stateBox);
        form.addRow(r++, new Label("Vote data"), electionBox);
        form.addRow(r++, new Label("Democratic seats"), demSpinner);
        form.add(demRange, 1, r++);
        form.addRow(r++, new Label("Min margin (pts)"), marginSpinner);
        form.addRow(r++, new Label("Seeds"), seedsSpinner);
        GridPane.setHgrow(stateBox, Priority.ALWAYS);
        demRange.getStyleClass().add("text-muted");
        
        runButton.setDefaultButton(true);
        runButton.getStyleClass().add("accent");
        runButton.setMaxWidth(Double.MAX_VALUE);
        cancelButton.setMaxWidth(Double.MAX_VALUE);
        cancelButton.setDisable(true);
        runButton.setOnAction(e -> run());
        cancelButton.setOnAction(e -> {
            if (running != null) {
                running.cancel(true); // interrupts the run, which cancels the chains' structured scope
            }
        });
        HBox buttons = new HBox(8, runButton, cancelButton);
        HBox.setHgrow(runButton, Priority.ALWAYS);
        HBox.setHgrow(cancelButton, Priority.ALWAYS);
        
        GridPane live = new GridPane();
        live.setHgap(8);
        live.setVgap(4);
        live.addRow(0, new Label("Phase"), phaseLabel);
        live.addRow(1, new Label("Flips/s"), flipsLabel);
        live.addRow(2, new Label("Score"), scoreLabel);
        live.addRow(3, new Label("D–R now"), seatsLabel);
        progressBar.setMaxWidth(Double.MAX_VALUE);
        
        importButton.setMaxWidth(Double.MAX_VALUE);
        importButton.setOnAction(e -> importFiles());
        pngButton.setOnAction(e -> export("png"));
        geoJsonButton.setOnAction(e -> export("geojson"));
        csvButton.setOnAction(e -> export("csv"));
        HBox exports = new HBox(6, pngButton, geoJsonButton, csvButton);
        setExportsDisabled(true);
        
        Label title = new Label("autoredistrict");
        title.getStyleClass().add("title-3");
        Label sub = new Label("Monte Carlo anneal after AlphaPhoenix's\n\"Elections made-to-order\"");
        sub.getStyleClass().add("text-muted");
        statusLabel.setWrapText(true);
        VBox box = new VBox(10, title, sub, new Separator(), form, buttons, progressBar, live, new Separator(), new Label("Export"), exports, new Separator(), importButton, statusLabel);
        box.setPadding(new Insets(16));
        box.setPrefWidth(360);
        box.setMinWidth(300);
        return box;
    }
    
    private VBox resultsPane() {
        table.getColumns().add(column("District", DistrictResult::district));
        table.getColumns().add(column("Population", d -> String.format(Locale.ROOT, "%,d", d.population())));
        table.getColumns().add(column("Deviation %", d -> String.format(Locale.ROOT, "%+.3f", d.deviation() * 100)));
        table.getColumns().add(column("D share", d -> String.format(Locale.ROOT, "%.1f%%", d.demShare() * 100)));
        table.getColumns().add(column("Winner", DistrictResult::winner));
        table.getColumns().add(column("Margin (pts)", d -> String.format(Locale.ROOT, "%+.1f", d.margin())));
        table.getColumns().add(column("Tracts", DistrictResult::tracts));
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        table.setPlaceholder(new Label("Run a plan to see its districts."));
        resultLabel.getStyleClass().add("title-4");
        VBox v = new VBox(6, resultLabel, table);
        v.setPadding(new Insets(8, 12, 12, 12));
        VBox.setVgrow(table, Priority.ALWAYS);
        return v;
    }
    
    private void refreshStates() {
        List<StateInfo> imported = new ArrayList<>();
        for (StateInfo s : States.all()) {
            if (TractCache.isImported(opts.cacheDir(), s)) {
                imported.add(s);
            }
        }
        StateInfo keep = stateBox.getValue();
        stateBox.setItems(FXCollections.observableArrayList(imported));
        if (keep != null && imported.contains(keep)) {
            stateBox.setValue(keep);
        }
        statusLabel.setText(imported.isEmpty() ? "No states imported yet: use Import RDH files…" : imported.size() + " states imported in " + opts.cacheDir().toAbsolutePath());
    }
    
    private void loadState(StateInfo s) {
        Election election = Election.parse(electionBox.getValue());
        // A newer selection replaces any load still running, and Run and Export wait for it.
        if (loading != null) {
            loading.cancel();
        }
        dataset = null;
        result = null;
        resultDataset = null;
        setExportsDisabled(true);
        Task<StateDataset> t = new Task<>() {
            @Override
            protected StateDataset call() {
                StateDataset ds = service.load(s, election);
                ds.geometry();
                ds.graph();
                return ds;
            }
        };
        loading = t;
        t.setOnSucceeded(e -> {
            if (t != loading) {
                return;
            }
            loading = null;
            dataset = t.getValue();
            result = null;
            setExportsDisabled(true);
            int k = s.seats();
            int prop = PlanOptions.proportional(dataset.demShare(), k);
            demSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(0, k, prop));
            demRange.setText(String.format(Locale.ROOT, "0..%d · proportional %d (D share %.1f%%)", k, prop, dataset.demShare() * 100));
            double[] shares = new double[dataset.size()];
            for (int i = 0; i < shares.length; i++) {
                double d = dataset.graph().dem(i);
                double r = dataset.graph().rep(i);
                shares[i] = d + r > 0 ? d / (d + r) : 0.5;
            }
            mapView.showState(dataset.geometry(), shares, Artifacts.populatedExtent(dataset));
            table.getItems().clear();
            resultLabel.setText(s.name() + ": " + dataset.size() + " tracts, " + k + " seat" + (k == 1 ? "" : "s") + ", votes " + dataset.voteDataLabel());
            statusLabel.setText(String.join("\n", dataset.warnings()));
            if (opts.snapshotState() != null && s.abbr().equalsIgnoreCase(opts.snapshotState())) {
                if (opts.demSeats() != null) {
                    demSpinner.getValueFactory().setValue(opts.demSeats());
                }
                if (opts.seeds() != null) {
                    seedsSpinner.getValueFactory().setValue(opts.seeds());
                }
                run();
            }
        });
        t.setOnFailed(e -> {
            if (t == loading) {
                loading = null;
                fail("Could not load " + s.abbr(), t.getException());
            }
        });
        Thread.ofPlatform().daemon().name("load-" + s.abbr()).start(t);
    }
    
    private void run() {
        if (dataset == null || running != null) {
            return;
        }
        PlanOptions o = PlanOptions.defaults().withDemSeats(demSpinner.getValue()).withMargin(marginSpinner.getValue()).withSeeds(seedsSpinner.getValue());
        StateDataset ds = dataset;
        Map<Integer, Double> fps = new ConcurrentHashMap<>();
        AtomicLong lastUi = new AtomicLong();
        Progress.Listener listener = p -> {
            fps.put(p.chain(), p.flipsPerSecond());
            long now = System.nanoTime();
            long prev = lastUi.get();
            if (p.chain() == 0 && now - prev > 100_000_000L && lastUi.compareAndSet(prev, now)) {
                double sum = fps.values().stream().mapToDouble(Double::doubleValue).sum();
                double frac = p.phaseSteps() > 0 ? (p.phaseIndex() + Math.min(1.0, (double) p.step() / p.phaseSteps())) / p.phaseCount() : 1.0;
                Platform.runLater(() -> {
                    phaseLabel.setText(p.phase() + " (" + Math.min(p.phaseIndex() + 1, p.phaseCount()) + "/" + p.phaseCount() + ")");
                    flipsLabel.setText(String.format(Locale.ROOT, "%,.0f", sum));
                    scoreLabel.setText(String.format(Locale.ROOT, "%.4f", p.score()));
                    seatsLabel.setText(p.demWon() + " D–" + (ds.state().seats() - p.demWon()) + " R, max dev " + String.format(Locale.ROOT, "%.2f%%", p.maxDeviation() * 100));
                    progressBar.setProgress(Math.min(1, frac));
                });
            }
        };
        Task<PlanResult> t = new Task<>() {
            @Override
            protected PlanResult call() throws Exception {
                return service.run(ds, o, listener);
            }
        };
        t.setOnSucceeded(e -> {
            running = null;
            setRunning(false);
            // The selectors are locked during a run, but if the selection changed anyway the
            // result belongs to another dataset than the one on screen: drop it.
            if (ds != dataset) {
                phaseLabel.setText("done (selection changed; result discarded)");
                return;
            }
            showResult(t.getValue(), ds);
            progressBar.setProgress(1);
            if (opts.snapshotState() != null) {
                // Let the table and map lay out, then capture the whole window.
                PauseTransition pause = new PauseTransition(javafx.util.Duration.millis(600));
                pause.setOnFinished(x -> saveSnapshot());
                pause.play();
            }
        });
        t.setOnCancelled(e -> {
            running = null;
            setRunning(false);
            phaseLabel.setText("cancelled");
            progressBar.setProgress(0);
        });
        t.setOnFailed(e -> {
            running = null;
            setRunning(false);
            fail("The run failed", t.getException());
        });
        running = t;
        setRunning(true);
        phaseLabel.setText("starting");
        Thread.ofPlatform().daemon().name("plan-run").start(t);
    }
    
    private void showResult(PlanResult r, StateDataset ds) {
        result = r;
        resultDataset = ds;
        mapView.showPlan(Artifacts.mapData(r, ds));
        table.setItems(FXCollections.observableArrayList(r.metrics().rows()));
        resultLabel.setText(String.format(Locale.ROOT, "%s: requested %d D · won %d D–%d R · %s · max deviation %.2f%% · closest margin %.1f pts " + "· %s · %.1f s", r.state().abbr(), r.demRequested(), r.metrics().demWon(), r.metrics().repWon(), r.hit() ? "HIT" : "miss", r.metrics().maxDeviation() * 100, r.metrics().closestMargin(), r.pass() ? "PASS" : "FAIL", r.seconds()));
        seatsLabel.setText(r.metrics().demWon() + " D–" + r.metrics().repWon() + " R");
        phaseLabel.setText("done");
        setExportsDisabled(false);
    }
    
    private void setRunning(boolean on) {
        runButton.setDisable(on);
        cancelButton.setDisable(!on);
        // Changing the state or the election reloads the dataset, so both wait for the run.
        stateBox.setDisable(on);
        electionBox.setDisable(on);
        importButton.setDisable(on);
    }
    
    private void setExportsDisabled(boolean d) {
        pngButton.setDisable(d);
        geoJsonButton.setDisable(d);
        csvButton.setDisable(d);
    }
    
    private void export(String kind) {
        if (result == null) {
            return;
        }
        FileChooser fc = new FileChooser();
        String stem = result.state().key() + "-" + result.demRequested() + "d";
        fc.setInitialFileName(stem + (kind.equals("csv") ? "-assignment.csv" : "." + kind));
        File f = fc.showSaveDialog(stage);
        if (f == null) {
            return;
        }
        try {
            switch (kind) {
                case "png" -> MapRenderer.writePng(Artifacts.png(mapView.plan(), 1600), f.toPath());
                case "geojson" ->
                        Exporters.geoJson(f.toPath(), mapView.plan().districtShapes(), Artifacts.props(result), g -> resultDataset.projection().toLonLat(g));
                default -> Exporters.assignmentCsv(f.toPath(), resultDataset.geoids(), result.assignment());
            }
            statusLabel.setText("Saved " + f);
        } catch (RuntimeException ex) {
            fail("Export failed", ex);
        }
    }
    
    private void importFiles() {
        FileChooser fc = new FileChooser();
        fc.setTitle("Select RDH tract zips (xx_pl2020_t.zip); vote files must be in the same folder");
        fc.getExtensionFilters().add(new FileChooser.ExtensionFilter("RDH PL 94-171 tract zips", "*_pl2020_t.zip"));
        List<File> files = fc.showOpenMultipleDialog(stage);
        if (files == null || files.isEmpty()) {
            return;
        }
        Path dir = files.getFirst().toPath().getParent();
        List<StateInfo> states = new ArrayList<>();
        for (File f : files) {
            States.find(f.getName().substring(0, 2)).ifPresent(states::add);
        }
        Task<String> t = new Task<>() {
            @Override
            protected String call() throws Exception {
                return ImportService.summaryTable(ImportService.importFolder(dir, states, opts.cacheDir(), 4, msg -> Platform.runLater(() -> statusLabel.setText(msg))));
            }
        };
        t.setOnSucceeded(e -> {
            setRunning(false);
            refreshStates();
            statusLabel.setText("Imported " + states.size() + " state(s) from " + dir);
        });
        t.setOnFailed(e -> {
            setRunning(false);
            fail("Import failed", t.getException());
        });
        setRunning(true);
        cancelButton.setDisable(true);
        statusLabel.setText("Importing " + states.size() + " state(s)…");
        Thread.ofPlatform().daemon().name("import").start(t);
    }
    
    private void startSnapshot() {
        StateInfo s = States.require(opts.snapshotState());
        if (!stateBox.getItems().contains(s)) {
            System.err.println(s.abbr() + " isn't imported in " + opts.cacheDir());
            Platform.exit();
            System.exit(3);
        }
        stateBox.setValue(s);
    }
    
    private void saveSnapshot() {
        WritableImage img = stage.getScene().snapshot(null);
        int w = (int) img.getWidth();
        int h = (int) img.getHeight();
        BufferedImage out = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
        PixelReader pr = img.getPixelReader();
        for (int y = 0; y < h; y++) {
            for (int x = 0; x < w; x++) {
                out.setRGB(x, y, pr.getArgb(x, y));
            }
        }
        try {
            Path file = opts.out().toAbsolutePath();
            MapRenderer.writePng(out, file);
            System.out.printf(Locale.ROOT, "snapshot %dx%d written to %s; %s requested %d D, won %d D-%d R (%s)%n", w, h, file, result.state().abbr(), result.demRequested(), result.metrics().demWon(), result.metrics().repWon(), result.hit() ? "hit" : "miss");
        } catch (RuntimeException e) {
            e.printStackTrace();
            Platform.exit();
            System.exit(1);
        }
        Platform.exit();
        System.exit(0);
    }
    
    private void fail(String what, Throwable ex) {
        Throwable cause = ex;
        while (cause.getCause() != null && cause.getMessage() == null) {
            cause = cause.getCause();
        }
        statusLabel.setText(what + ": " + cause.getMessage());
        if (opts.snapshotState() != null) {
            System.err.println(what + ": " + cause);
            Platform.exit();
            System.exit(1);
        }
        Alert a = new Alert(Alert.AlertType.ERROR, cause.getMessage());
        a.setHeaderText(what);
        a.show();
    }
    
    private static final class StateCell extends ListCell<StateInfo> {
        @Override
        protected void updateItem(StateInfo s, boolean empty) {
            super.updateItem(s, empty);
            setText(empty || s == null ? null : s.abbr() + " — " + s.name() + " (" + s.seats() + ")");
        }
    }
}
