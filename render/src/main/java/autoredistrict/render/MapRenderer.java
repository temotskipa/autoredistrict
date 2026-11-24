package autoredistrict.render;

import org.locationtech.jts.geom.Envelope;

import javax.imageio.ImageIO;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/**
 * Headless Java2D map PNGs and the all-states contact sheet. No FX toolkit involved.
 */
public final class MapRenderer {
    
    private MapRenderer() {
    }
    
    /**
     * A full map with title, legend and district labels.
     */
    public static BufferedImage render(MapData data, int width, int height) {
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        try {
            Java2DCanvas c = new Java2DCanvas(g);
            c.fillRect(0, 0, width, height, MapPainter.BACKGROUND);
            double header = 64;
            Envelope env = data.bounds();
            MapPainter.Viewport vp = MapPainter.Viewport.fit(env, 0, header, width, height - header, 24);
            double label = Math.clamp(260.0 / Math.sqrt(Math.max(1, data.districts())), 11, 18);
            // Keep the map (water tracts can reach past the populated frame) out of the header band.
            g.setClip(0, (int) header, width, height - (int) header);
            MapPainter.paint(c, data, vp, true, label);
            g.setClip(null);
            MapPainter.header(c, data, width, 16);
        } finally {
            g.dispose();
        }
        return img;
    }
    
    /**
     * A thumbnail with a one-line caption and no district labels.
     */
    public static BufferedImage thumbnail(MapData data, String caption, int width, int height) {
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        try {
            Java2DCanvas c = new Java2DCanvas(g);
            c.fillRect(0, 0, width, height, 0xFFFFFFFF);
            double captionH = 22;
            MapPainter.Viewport vp = MapPainter.Viewport.fit(data.bounds(), 0, captionH, width, height - captionH, 6);
            MapPainter.paint(c, data, vp, false, 9);
            c.text(caption, 6, 16, MapPainter.TEXT, 13, true, false, false);
        } finally {
            g.dispose();
        }
        return img;
    }
    
    /**
     * Lays the tiles out in a labelled grid.
     */
    public static BufferedImage contactSheet(String title, List<Tile> tiles, int columns) {
        if (tiles.isEmpty()) {
            throw new IllegalArgumentException("no tiles");
        }
        int tw = tiles.getFirst().image().getWidth();
        int th = tiles.getFirst().image().getHeight();
        int rows = (tiles.size() + columns - 1) / columns;
        int gap = 6;
        int top = 44;
        int width = columns * tw + (columns + 1) * gap;
        int height = top + rows * th + (rows + 1) * gap;
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        try {
            Java2DCanvas c = new Java2DCanvas(g);
            c.fillRect(0, 0, width, height, 0xFFDDDDDD);
            c.text(title, gap + 4, 30, MapPainter.TEXT, 22, true, false, false);
            for (int i = 0; i < tiles.size(); i++) {
                int col = i % columns;
                int row = i / columns;
                g.drawImage(tiles.get(i).image(), gap + col * (tw + gap), top + gap + row * (th + gap), null);
            }
        } finally {
            g.dispose();
        }
        return img;
    }
    
    public static void writePng(BufferedImage img, Path file) {
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
            if (!ImageIO.write(img, "png", file.toFile())) {
                throw new IOException("no PNG writer");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write " + file, e);
        }
    }
    
    /**
     * "NC · 14 districts · requested 7 D · won 7 D–7 R" style caption pieces.
     */
    public static String seats(int dem, int rep) {
        return String.format(Locale.ROOT, "%d D–%d R", dem, rep);
    }
    
    /**
     * One tile of the contact sheet.
     */
    public record Tile(String caption, BufferedImage image) {
    }
}
