package com.temotskipa.autoredistrict.render;

import java.awt.*;
import java.awt.geom.Path2D;
import java.awt.geom.Rectangle2D;

/**
 * {@link Canvas2D} on a Java2D {@link Graphics2D}, for headless PNGs.
 */
public final class Java2DCanvas implements Canvas2D {
    
    private final Graphics2D g;
    private Path2D.Double path = new Path2D.Double(Path2D.WIND_EVEN_ODD);
    
    public Java2DCanvas(Graphics2D g) {
        this.g = g;
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_STROKE_CONTROL, RenderingHints.VALUE_STROKE_PURE);
        g.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
    }
    
    @Override
    public void beginPath() {
        path = new Path2D.Double(Path2D.WIND_EVEN_ODD);
    }
    
    @Override
    public void moveTo(double x, double y) {
        path.moveTo(x, y);
    }
    
    @Override
    public void lineTo(double x, double y) {
        path.lineTo(x, y);
    }
    
    @Override
    public void closePath() {
        path.closePath();
    }
    
    @Override
    public void fill(int argb) {
        g.setColor(new Color(argb, true));
        g.fill(path);
    }
    
    @Override
    public void stroke(int argb, double width) {
        g.setColor(new Color(argb, true));
        g.setStroke(new BasicStroke((float) width, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
        g.draw(path);
    }
    
    @Override
    public void text(String s, double x, double y, int argb, double size, boolean bold, boolean centered, boolean halo) {
        var font = new Font(Font.SANS_SERIF, bold ? Font.BOLD : Font.PLAIN, (int) Math.round(size));
        g.setFont(font);
        FontMetrics fm = g.getFontMetrics();
        Rectangle2D bounds = fm.getStringBounds(s, g);
        double tx = centered ? x - bounds.getWidth() / 2 : x;
        double ty = centered ? y + (fm.getAscent() - fm.getDescent()) / 2.0 : y;
        if (halo) {
            g.setColor(new Color(255, 255, 255, 200));
            for (int dx = -1; dx <= 1; dx++) {
                for (int dy = -1; dy <= 1; dy++) {
                    if (dx != 0 || dy != 0) {
                        g.drawString(s, (float) tx + dx, (float) ty + dy);
                    }
                }
            }
        }
        g.setColor(new Color(argb, true));
        g.drawString(s, (float) tx, (float) ty);
    }
    
    @Override
    public void fillRect(double x, double y, double w, double h, int argb) {
        g.setColor(new Color(argb, true));
        g.fill(new Rectangle2D.Double(x, y, w, h));
    }
}
