package autoredistrict.data;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.ItemDistance;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.ArrayList;
import java.util.List;

/**
 * Makes the unit graph connected. Islands (HI, AK, the Florida Keys, ME and MA islands...) and
 * water gaps leave the rook graph in several components. Each component other than the largest is
 * linked to the nearest unit outside it, measured as the distance between projected geometries,
 * with a "bridge" edge of shared length 0. Rounds repeat until one component is left. Bridges
 * count for contiguity, like the water contiguity that real plans use, but add no boundary length.
 */
public final class Connectivity {
    
    private Connectivity() {
    }
    
    public static Repair connect(List<Geometry> projected, int[] edgeA, int[] edgeB) {
        int n = projected.size();
        UnionFind uf = new UnionFind(n);
        for (int e = 0; e < edgeA.length; e++) {
            uf.union(edgeA[e], edgeB[e]);
        }
        int before = uf.count();
        List<Bridge> bridges = new ArrayList<>();
        if (before <= 1) {
            return new Repair(bridges, before);
        }
        STRtree tree = new STRtree();
        for (int i = 0; i < n; i++) {
            tree.insert(projected.get(i).getEnvelopeInternal(), i);
        }
        tree.build();
        while (uf.count() > 1) {
            // Group units by component root and find the largest component.
            int[] size = new int[n];
            for (int i = 0; i < n; i++) {
                size[uf.find(i)]++;
            }
            int largest = 0;
            for (int i = 1; i < n; i++) {
                if (size[i] > size[largest]) {
                    largest = i;
                }
            }
            List<List<Integer>> comps = new ArrayList<>();
            int[] index = new int[n];
            java.util.Arrays.fill(index, -1);
            for (int i = 0; i < n; i++) {
                int r = uf.find(i);
                if (r == largest) {
                    continue;
                }
                if (index[r] < 0) {
                    index[r] = comps.size();
                    comps.add(new ArrayList<>());
                }
                comps.get(index[r]).add(i);
            }
            for (List<Integer> comp : comps) {
                int root = uf.find(comp.getFirst());
                if (uf.count() == 1) {
                    break;
                }
                double best = Double.POSITIVE_INFINITY;
                int bestA = -1;
                int bestB = -1;
                for (int u : comp) {
                    final int compRoot = root;
                    ItemDistance foreign = (ItemBoundable x, ItemBoundable y) -> {
                        int i = (Integer) x.getItem();
                        int j = (Integer) y.getItem();
                        if (uf.find(i) == compRoot && uf.find(j) == compRoot) {
                            return Double.POSITIVE_INFINITY;
                        }
                        return projected.get(i).distance(projected.get(j));
                    };
                    Object nearest = tree.nearestNeighbour(projected.get(u).getEnvelopeInternal(), u, foreign);
                    if (nearest == null) {
                        continue;
                    }
                    int v = (Integer) nearest;
                    if (uf.find(v) == root) {
                        continue;
                    }
                    double d = projected.get(u).distance(projected.get(v));
                    if (d < best) {
                        best = d;
                        bestA = u;
                        bestB = v;
                    }
                }
                if (bestA < 0) {
                    throw new DataException("Could not link a component of " + comp.size() + " units to the rest.");
                }
                bridges.add(new Bridge(Math.min(bestA, bestB), Math.max(bestA, bestB), best));
                uf.union(bestA, bestB);
            }
        }
        return new Repair(bridges, before);
    }
    
    /**
     * A bridge edge between two units, with the gap it spans in metres. A value object.
     */
    public value record Bridge(int a, int b, double gapMetres) {
    }
    
    /**
     * Result: the bridges added and how many components there were before.
     */
    public record Repair(List<Bridge> bridges, int componentsBefore) {
    }
    
    static final class UnionFind {
        private final int[] parent;
        private int count;
        
        UnionFind(int n) {
            parent = new int[n];
            for (int i = 0; i < n; i++) {
                parent[i] = i;
            }
            count = n;
        }
        
        int find(int x) {
            while (parent[x] != x) {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            return x;
        }
        
        void union(int a, int b) {
            int ra = find(a);
            int rb = find(b);
            if (ra != rb) {
                parent[ra] = rb;
                count--;
            }
        }
        
        int count() {
            return count;
        }
    }
}
