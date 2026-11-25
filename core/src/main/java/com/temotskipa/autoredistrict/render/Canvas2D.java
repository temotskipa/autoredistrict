package com.temotskipa.autoredistrict.render;

/**
 * The drawing operations the map painter needs, so the same painting code draws headless PNGs
 * (Java2D, {@link Java2DCanvas}) and the GUI's JavaFX canvas. Colours are ARGB ints; paths use
 * the even-odd rule so polygon holes stay empty.
 */
public interface Canvas2D {
    
    void beginPath();
    
    void moveTo(double x, double y);
    
    void lineTo(double x, double y);
    
    void closePath();
    
    void fill(int argb);
    
    void stroke(int argb, double width);
    
    /**
     * Draws text with its baseline-left at (x, y), optionally centred and with a light halo.
     */
    void text(String s, double x, double y, int argb, double size, boolean bold, boolean centered, boolean halo);
    
    void fillRect(double x, double y, double w, double h, int argb);
}
