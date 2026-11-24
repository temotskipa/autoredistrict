package com.temotskipa.autoredistrict.engine;

/**
 * An axis-aligned box in projected metres. A value object.
 */
public value record BoundingBox(double minX, double minY, double maxX, double maxY) {
    
    public static final BoundingBox EMPTY = new BoundingBox(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    
    public BoundingBox include(double x, double y) {
        return new BoundingBox(Math.min(minX, x), Math.min(minY, y), Math.max(maxX, x), Math.max(maxY, y));
    }
    
    public BoundingBox union(BoundingBox o) {
        return new BoundingBox(Math.min(minX, o.minX), Math.min(minY, o.minY), Math.max(maxX, o.maxX), Math.max(maxY, o.maxY));
    }
    
    public double width() {
        return maxX - minX;
    }
    
    public double height() {
        return maxY - minY;
    }
    
    public boolean isEmpty() {
        return minX > maxX || minY > maxY;
    }
    
    public Point center() {
        return new Point((minX + maxX) / 2, (minY + maxY) / 2);
    }
}
