package com.temotskipa.autoredistrict.engine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PointAndBoundingBoxTest {
    
    @Test
    void aBoxGrowsToIncludeItsPointsAndFindsItsCentre() {
        assertTrue(BoundingBox.EMPTY.isEmpty());
        BoundingBox box = BoundingBox.EMPTY.include(0, 0).include(10, 5);
        assertFalse(box.isEmpty());
        assertEquals(new BoundingBox(0, 0, 10, 5), box);
        assertEquals(new Point(5, 2.5), box.center());
    }
    
    @Test
    void pointsMeasureStraightLineDistance() {
        assertEquals(5.0, new Point(0, 0).distance(new Point(3, 4)), 1e-12);
        assertEquals(0.0, new Point(1.5, -2.0).distance(new Point(1.5, -2.0)), 0.0);
    }
}
