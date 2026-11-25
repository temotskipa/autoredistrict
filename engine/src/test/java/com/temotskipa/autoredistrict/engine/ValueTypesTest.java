package com.temotskipa.autoredistrict.engine;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The engine's small immutable types are JEP 401 value classes: no identity, == compares state.
 */
class ValueTypesTest {
    
    @Test
    void smallTypesAreValueClasses() {
        for (Class<?> c : List.of(Point.class, BoundingBox.class, Edge.class, Weights.class, Target.class, Progress.class, PhaseStats.class)) {
            assertTrue(c.isValue(), c + " should be a value class");
        }
        assertFalse(Graph.class.isValue());
        assertFalse(ChainResult.class.isValue());
    }
    
    @Test
    void equalStateMeansDoubleEquals() {
        assertSame(new Point(1.5, -2.0), new Point(1.5, -2.0));
        assertNotSame(new Point(1.5, -2.0), new Point(1.5, 2.0));
        assertSame(new Edge(1, 2, 30.0), new Edge(1, 2, 30.0));
        assertSame(new Target(14, 7, 2.0, 0.01), new Target(14, 7, 2.0, 0.01));
        BoundingBox box = BoundingBox.EMPTY.include(0, 0).include(10, 5);
        assertSame(new BoundingBox(0, 0, 10, 5), box);
        assertEquals(new Point(5, 2.5), box.center());
        assertEquals(5.0, new Point(0, 0).distance(new Point(3, 4)), 1e-12);
    }
}
