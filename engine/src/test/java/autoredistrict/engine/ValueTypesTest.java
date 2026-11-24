package autoredistrict.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ValueTypesTest {

    @Test
    void pointIsAValueClassComparedByState() {
        assertTrue(Point.class.isValue());
        Point a = new Point(1.5, -2.0);
        Point b = new Point(1.5, -2.0);
        assertTrue(a == b, "value objects with equal fields are ==");
        assertEquals(5.0, new Point(0, 0).distance(new Point(3, 4)), 1e-12);
    }
}
