package com.temotskipa.autoredistrict.gui;

import com.temotskipa.autoredistrict.render.MapData;
import com.temotskipa.autoredistrict.render.MapPainter;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.layout.Pane;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * The map on a JavaFX {@link Canvas}: scroll to zoom around the pointer, drag to pan,
 * double-click to fit. Shows either a state's tracts shaded by their own D share (before a run)
 * or a plan's districts.
 */
final class MapView extends Pane {
    
    private final Canvas canvas = new Canvas();
    private List<Geometry> tracts;
    private double[] tractShares;
    private MapData plan;
    private Envelope bounds;
    private MapPainter.Viewport viewport;
    private double dragX;
    private double dragY;
    
    MapView() {
        getChildren().add(canvas);
        setStyle("-fx-background-color: #f7f7f5;");
        setMinSize(300, 300);
        widthProperty().addListener((_, _, _) -> resize());
        heightProperty().addListener((_, _, _) -> resize());
        setOnScroll(e -> {
            if (viewport != null) {
                double f = Math.pow(1.0015, e.getDeltaY());
                viewport = viewport.zoom(f, e.getX(), e.getY());
                redraw();
            }
        });
        setOnMousePressed(e -> {
            dragX = e.getX();
            dragY = e.getY();
        });
        setOnMouseDragged(e -> {
            // getButton() names the button whose state changed, which is none during a drag.
            if (viewport != null && e.isPrimaryButtonDown()) {
                viewport = viewport.pan(e.getX() - dragX, e.getY() - dragY);
                dragX = e.getX();
                dragY = e.getY();
                redraw();
            }
        });
        setOnMouseClicked(e -> {
            if (e.getClickCount() == 2) {
                fit();
            }
        });
    }
    
    /**
     * Shows a state's tracts, shaded by their own D share, framed on {@code focus} (the populated
     * tracts) when given, so remote empty tracts such as Hawaii's northwestern islands don't shrink it.
     */
    void showState(List<Geometry> tracts, double[] shares, Envelope focus) {
        this.tracts = tracts;
        this.tractShares = shares;
        this.plan = null;
        Envelope env = new Envelope();
        if (focus != null && !focus.isNull()) {
            env.expandToInclude(focus);
        }
        else {
            for (Geometry g : tracts) {
                env.expandToInclude(g.getEnvelopeInternal());
            }
        }
        this.bounds = env;
        fit();
    }

    /**
     * Shows a finished plan, framed like the PNG export. Keeps the user's zoom and pan when the plan
     * has the same frame as what is on screen.
     */
    void showPlan(MapData data) {
        this.plan = data;
        Envelope frame = data.bounds();
        if (bounds == null || !bounds.equals(frame)) {
            bounds = frame;
            fit();
        }
        else {
            redraw();
        }
    }
    
    MapData plan() {
        return plan;
    }
    
    private void resize() {
        canvas.setWidth(getWidth());
        canvas.setHeight(getHeight());
        fit();
    }
    
    void fit() {
        if (bounds != null && getWidth() > 0 && getHeight() > 0) {
            viewport = MapPainter.Viewport.fit(bounds, 0, 0, getWidth(), getHeight(), 20);
        }
        redraw();
    }
    
    private void redraw() {
        GraphicsContext g = canvas.getGraphicsContext2D();
        FxCanvas c = new FxCanvas(g);
        c.fillRect(0, 0, canvas.getWidth(), canvas.getHeight(), MapPainter.BACKGROUND);
        if (viewport == null) {
            return;
        }
        if (plan != null) {
            double label = Math.clamp(260.0 / Math.sqrt(Math.max(1, plan.districts())), 11, 18);
            MapPainter.paint(c, plan, viewport, true, label);
        }
        else if (tracts != null) {
            for (int i = 0; i < tracts.size(); i++) {
                int col = MapPainter.shareColor(tractShares[i]);
                MapPainter.path(c, tracts.get(i), viewport);
                c.fill(col);
                c.stroke(0x40FFFFFF, 0.5);
            }
        }
    }
}
