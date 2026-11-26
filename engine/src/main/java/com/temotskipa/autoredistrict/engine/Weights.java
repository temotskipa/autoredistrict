package com.temotskipa.autoredistrict.engine;

/**
 * Score weights for one phase. A value object.
 */
public value record Weights(double population, double compactness, double tension, double election) {
}
