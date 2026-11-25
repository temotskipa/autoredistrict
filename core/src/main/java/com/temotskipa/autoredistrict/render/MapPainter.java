package com.temotskipa.autoredistrict.render;

import org.locationtech.jts.geom.*;

import java.util.Locale;

/**
 * Paints a plan onto any {@link Canvas2D}: tracts filled by their district's D share (blue for D
 * wins, red for R wins, paler near 50%), district outlines, the state outline, and a numbered
 * label per district. Shared by the headless PNG renderer and the GUI.
 */
public final class MapPainter {
    
    public static final int BACKGROUND = 0xFFF7F7F5;
    public static final int OUTLINE = 0xFF262626;
    public static final int TEXT = 0xFF1A1A1A;
    
    private MapPainter() {
    }
    
    /**
     * Colour {@code a} faded 65% toward the background, for tracts with no people.
     */
    static int faded(int a) {
        int b = BACKGROUND;
        double t = 0.65;
        int r = (int) Math.round(((a >> 16) & 0xFF) * (1 - t) + ((b >> 16) & 0xFF) * t);
        int g = (int) Math.round(((a >> 8) & 0xFF) * (1 - t) + ((b >> 8) & 0xFF) * t);
        int bl = (int) Math.round((a & 0xFF) * (1 - t) + (b & 0xFF) * t);
        return 0xFF000000 | (r << 16) | (g << 8) | bl;
    }
    
    /**
     * D share to colour: red below 50%, blue above, saturating 20 points from even.
     */
    public static int shareColor(double share) {
        double t = Math.clamp((share - 0.5) / 0.2, -1, 1);
        int[] white = {0xF2, 0xF0, 0xEC};
        int[] blue = {0x21, 0x66, 0xAC};
        int[] red = {0xB2, 0x18, 0x2B};
        int[] end = t >= 0 ? blue : red;
        double a = 0.25 + 0.75 * Math.abs(t);
        if (Math.abs(t) < 1e-9) {
            a = 0.12;
        }
        int r = (int) Math.round(white[0] + (end[0] - white[0]) * a);
        int g = (int) Math.round(white[1] + (end[1] - white[1]) * a);
        int b = (int) Math.round(white[2] + (end[2] - white[2]) * a);
        return 0xFF000000 | (r << 16) | (g << 8) | b;
    }
    
    /**
     * Paints the plan; {@code labels} adds district numbers.
     */
    public static void paint(Canvas2D c, MapData data, Viewport vp, boolean labels, double labelSize) {
        int[] color = new int[data.districts()];
        for (int d = 0; d < data.districts(); d++) {
            color[d] = shareColor(data.demShare()[d]);
        }
        // Tract fills, stroked in the same colour to hide hairline gaps between neighbours.
        for (int i = 0; i < data.assignment().length; i++) {
            Geometry g = data.tracts().get(i);
            int col = color[data.assignment()[i]];
            if (data.isEmpty(i)) {
                col = faded(col);
            }
            path(c, g, vp);
            c.fill(col);
            c.stroke(col, 0.6);
        }
        // District outlines.
        double lw = Math.clamp(labelSize / 9, 0.8, 2.2);
        for (Geometry g : data.districtShapes()) {
            path(c, g, vp);
            c.stroke(0xFFFFFFFF, lw + 1.4);
        }
        for (Geometry g : data.districtShapes()) {
            path(c, g, vp);
            c.stroke(OUTLINE, lw);
        }
        if (labels) {
            for (int d = 0; d < data.districts(); d++) {
                Geometry g = data.districtShapes()[d];
                if (g.isEmpty()) {
                    continue;
                }
                Geometry largest = largestPart(g);
                Point p = largest.getInteriorPoint();
                c.text(Integer.toString(d + 1), vp.px(p.getX()), vp.py(p.getY()), TEXT, labelSize, true, true, true);
            }
        }
    }
    
    /**
     * Title, subtitle and a share legend across the top of a {@code w}-wide image.
     */
    public static void header(Canvas2D c, MapData data, double w, double size) {
        c.text(data.title(), 16, 16 + size, TEXT, size * 1.25, true, false, false);
        if (data.subtitle() != null && !data.subtitle().isEmpty()) {
            c.text(data.subtitle(), 16, 22 + size * 2.3, 0xFF444444, size * 0.85, false, false, false);
        }
        double lx = w - 16 - 7 * 34;
        double ly = 14;
        double[] shares = {0.30, 0.40, 0.47, 0.50, 0.53, 0.60, 0.70};
        for (int i = 0; i < shares.length; i++) {
            c.fillRect(lx + i * 34, ly, 34, 14, shareColor(shares[i]));
            c.text(String.format(Locale.ROOT, "%.0f", shares[i] * 100), lx + i * 34 + 17, ly + 26, 0xFF444444, size * 0.7, false, true, false);
        }
        c.text("D share %", lx - 6 - size * 4.2, ly + 12, 0xFF444444, size * 0.7, false, false, false);
    }
    
    private static Geometry largestPart(Geometry g) {
        Geometry best = g.getGeometryN(0);
        for (int i = 1; i < g.getNumGeometries(); i++) {
            if (g.getGeometryN(i).getArea() > best.getArea()) {
                best = g.getGeometryN(i);
            }
        }
        return best;
    }
    
    /**
     * Adds a (multi)polygon's rings to the canvas path.
     */
    public static void path(Canvas2D c, Geometry g, Viewport vp) {
        c.beginPath();
        for (int i = 0; i < g.getNumGeometries(); i++) {
            Geometry part = g.getGeometryN(i);
            if (part instanceof Polygon poly) {
                ring(c, poly.getExteriorRing(), vp);
                for (int h = 0; h < poly.getNumInteriorRing(); h++) {
                    ring(c, poly.getInteriorRingN(h), vp);
                }
            }
        }
    }
    
    private static void ring(Canvas2D c, LineString ring, Viewport vp) {
        Coordinate[] cs = ring.getCoordinates();
        if (cs.length < 3) {
            return;
        }
        double lastX = vp.px(cs[0].x);
        double lastY = vp.py(cs[0].y);
        c.moveTo(lastX, lastY);
        for (int j = 1; j < cs.length; j++) {
            double x = vp.px(cs[j].x);
            double y = vp.py(cs[j].y);
            // Skip vertices closer than a quarter pixel; they add nothing at this scale.
            if (Math.abs(x - lastX) + Math.abs(y - lastY) >= 0.25 || j == cs.length - 1) {
                c.lineTo(x, y);
                lastX = x;
                lastY = y;
            }
        }
        c.closePath();
    }
    
    /**
     * Maps projected metres to canvas pixels: px = ox + s*x, py = oy - s*y. A value object.
     */
    public value record Viewport(double scale, double offsetX, double offsetY) {
        
        /**
         * Fits the envelope into a w x h box with a margin, centred.
         */
        public static Viewport fit(Envelope env, double x0, double y0, double w, double h, double margin) {
            double sw = (w - 2 * margin) / Math.max(1, env.getWidth());
            double sh = (h - 2 * margin) / Math.max(1, env.getHeight());
            double s = Math.min(sw, sh);
            double ox = x0 + (w - s * env.getWidth()) / 2 - s * env.getMinX();
            double oy = y0 + (h + s * env.getHeight()) / 2 + s * env.getMinY();
            return new Viewport(s, ox, oy);
        }
        
        public double px(double x) {
            return offsetX + scale * x;
        }
        
        public double py(double y) {
            return offsetY - scale * y;
        }
        
        public double x(double px) {
            return (px - offsetX) / scale;
        }
        
        public double y(double py) {
            return (offsetY - py) / scale;
        }
        
        public Viewport zoom(double factor, double px, double py) {
            return new Viewport(scale * factor, px - (px - offsetX) * factor, py - (py - offsetY) * factor);
        }
        
        public Viewport pan(double dx, double dy) {
            return new Viewport(scale, offsetX + dx, offsetY + dy);
        }
    }
}
