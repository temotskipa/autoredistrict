package com.temotskipa.autoredistrict.engine;

/**
 * A point in a state's equal-area projection, in metres. A value object (JEP 401).
 */
public value record Point(double x, double y) {
    
    public double distance(Point other) {
        return Math.hypot(x - other.x, y - other.y);
    }
}
