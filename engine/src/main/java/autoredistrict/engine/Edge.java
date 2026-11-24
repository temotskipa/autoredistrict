package autoredistrict.engine;

/**
 * An undirected graph edge with its shared boundary length in metres (0 for bridges). A value object.
 */
public value record Edge(int a, int b, double sharedLength) {
    
    public Edge {
        if (a == b) {
            throw new IllegalArgumentException("self loop");
        }
    }
    
    public boolean isBridge() {
        return sharedLength == 0;
    }
}
