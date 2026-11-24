package com.temotskipa.autoredistrict.gui;

import com.temotskipa.autoredistrict.render.Canvas2D;
import javafx.geometry.VPos;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.paint.Color;
import javafx.scene.shape.FillRule;
import javafx.scene.shape.StrokeLineCap;
import javafx.scene.shape.StrokeLineJoin;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.text.TextAlignment;

/**
 * {@link Canvas2D} on a JavaFX {@link GraphicsContext}, so the GUI draws with the shared MapPainter.
 */
final class FxCanvas implements Canvas2D {
    
    private final GraphicsContext g;
    
    FxCanvas(GraphicsContext g) {
        this.g = g;
        g.setFillRule(FillRule.EVEN_ODD);
        g.setLineJoin(StrokeLineJoin.ROUND);
        g.setLineCap(StrokeLineCap.ROUND);
    }
    
    static Color color(int argb) {
        return Color.rgb((argb >> 16) & 0xFF, (argb >> 8) & 0xFF, argb & 0xFF, ((argb >>> 24) & 0xFF) / 255.0);
    }
    
    @Override
    public void beginPath() {
        g.beginPath();
    }
    
    @Override
    public void moveTo(double x, double y) {
        g.moveTo(x, y);
    }
    
    @Override
    public void lineTo(double x, double y) {
        g.lineTo(x, y);
    }
    
    @Override
    public void closePath() {
        g.closePath();
    }
    
    @Override
    public void fill(int argb) {
        g.setFill(color(argb));
        g.fill();
    }
    
    @Override
    public void stroke(int argb, double width) {
        g.setStroke(color(argb));
        g.setLineWidth(width);
        g.stroke();
    }
    
    @Override
    public void text(String s, double x, double y, int argb, double size, boolean bold, boolean centered, boolean halo) {
        g.setFont(Font.font("System", bold ? FontWeight.BOLD : FontWeight.NORMAL, size));
        g.setTextAlign(centered ? TextAlignment.CENTER : TextAlignment.LEFT);
        g.setTextBaseline(centered ? VPos.CENTER : VPos.BASELINE);
        if (halo) {
            g.setStroke(Color.rgb(255, 255, 255, 0.85));
            g.setLineWidth(3);
            g.strokeText(s, x, y);
        }
        g.setFill(color(argb));
        g.fillText(s, x, y);
    }
    
    @Override
    public void fillRect(double x, double y, double w, double h, int argb) {
        g.setFill(color(argb));
        g.fillRect(x, y, w, h);
    }
}
