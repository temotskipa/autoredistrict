package autoredistrict.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

/** The engine's small immutable types are JEP 401 value classes: no identity, == compares state. */
class ValueTypesTest {

    @Test
    void smallTypesAreValueClasses() {
        for (Class<?> c : List.of(Point.class, BoundingBox.class, Edge.class, Weights.class, Target.class,
                Progress.class, PhaseStats.class)) {
            assertTrue(c.isValue(), c + " should be a value class");
        }
        assertFalse(Graph.class.isValue());
        assertFalse(ChainResult.class.isValue());
    }

    @Test
    void equalStateMeansDoubleEquals() {
        assertTrue(new Point(1.5, -2.0) == new Point(1.5, -2.0));
        assertFalse(new Point(1.5, -2.0) == new Point(1.5, 2.0));
        assertTrue(new Edge(1, 2, 30.0) == new Edge(1, 2, 30.0));
        assertTrue(new Target(14, 7, 2.0, 0.01) == new Target(14, 7, 2.0, 0.01));
        BoundingBox box = BoundingBox.EMPTY.include(0, 0).include(10, 5);
        assertTrue(box == new BoundingBox(0, 0, 10, 5));
        assertEquals(new Point(5, 2.5), box.center());
        assertEquals(5.0, new Point(0, 0).distance(new Point(3, 4)), 1e-12);
    }
}
