package autoredistrict.engine;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.function.IntToDoubleFunction;

/** Synthetic lattice graphs and a simple contiguous starting map for engine tests. */
final class Grids {

    static final double CELL = 1000.0;

    private Grids() {
    }

    /**
     * A w x h rook lattice of 1 km cells, each with population {@code pop} and D share given by
     * {@code demShare(index)} over 100 votes.
     */
    static Graph lattice(int w, int h, int pop, IntToDoubleFunction demShare) {
        int n = w * h;
        int[] p = new int[n];
        double[] dem = new double[n];
        double[] rep = new double[n];
        double[] x = new double[n];
        double[] y = new double[n];
        double[] area = new double[n];
        double[] outer = new double[n];
        int edges = (w - 1) * h + w * (h - 1);
        int[] ea = new int[edges];
        int[] eb = new int[edges];
        double[] el = new double[edges];
        int m = 0;
        for (int r = 0; r < h; r++) {
            for (int c = 0; c < w; c++) {
                int i = r * w + c;
                p[i] = pop;
                double s = demShare.applyAsDouble(i);
                dem[i] = 100 * s;
                rep[i] = 100 * (1 - s);
                x[i] = c * CELL;
                y[i] = r * CELL;
                area[i] = CELL * CELL;
                outer[i] = CELL * ((c == 0 ? 1 : 0) + (c == w - 1 ? 1 : 0) + (r == 0 ? 1 : 0) + (r == h - 1 ? 1 : 0));
                if (c + 1 < w) {
                    ea[m] = i;
                    eb[m] = i + 1;
                    el[m++] = CELL;
                }
                if (r + 1 < h) {
                    ea[m] = i;
                    eb[m] = i + w;
                    el[m++] = CELL;
                }
            }
        }
        return Graph.of(p, dem, rep, x, y, area, outer, ea, eb, el);
    }

    /** Multi-source BFS from k random seed units: contiguous and deliberately uneven. */
    static final InitialMap BFS_VORONOI = (g, k, rng) -> {
        int n = g.size();
        int[] a = new int[n];
        Arrays.fill(a, -1);
        ArrayDeque<Integer> q = new ArrayDeque<>();
        int placed = 0;
        while (placed < k) {
            int s = rng.nextInt(n);
            if (a[s] < 0) {
                a[s] = placed++;
                q.add(s);
            }
        }
        while (!q.isEmpty()) {
            int u = q.poll();
            for (int v : g.neighborsOf(u)) {
                if (a[v] < 0) {
                    a[v] = a[u];
                    q.add(v);
                }
            }
        }
        return a;
    };

    /** Full BFS: is district d connected when unit skip is removed? */
    static boolean connectedWithout(Graph g, int[] assign, int d, int skip) {
        int start = -1;
        int size = 0;
        for (int i = 0; i < g.size(); i++) {
            if (assign[i] == d && i != skip) {
                size++;
                start = i;
            }
        }
        if (size == 0) {
            return true;
        }
        boolean[] seen = new boolean[g.size()];
        ArrayDeque<Integer> q = new ArrayDeque<>(List.of(start));
        seen[start] = true;
        int reached = 1;
        while (!q.isEmpty()) {
            int u = q.poll();
            for (int v : g.neighborsOf(u)) {
                if (!seen[v] && v != skip && assign[v] == d) {
                    seen[v] = true;
                    reached++;
                    q.add(v);
                }
            }
        }
        return reached == size;
    }

    static Schedule schedule(long perPhase) {
        return new Schedule(List.of(
                new Phase("shape", perPhase, new Weights(1, 50, 2, 0), 0.5, 1e-3, 1.0),
                new Phase("explore", perPhase, new Weights(1, 20, 0.5, 0.3), 0.8, 1e-2, 0.25),
                new Phase("elect", 2 * perPhase, new Weights(1, 20, 2, 1), 0.5, 1e-4, 1.0)),
                4 * perPhase, null);
    }

    static SplittableRandom rng(long seed) {
        return new SplittableRandom(seed);
    }
}
