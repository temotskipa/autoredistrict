package autoredistrict.service;

import autoredistrict.engine.Graph;

import java.util.ArrayDeque;

/**
 * Cosmetic clean-up after the anneal: a tract with no people and no votes (water, parks) moves
 * to the neighbouring district it shares the most boundary with, when both districts stay
 * contiguous. The anneal has no reason to place such tracts well (a ring of coastal water around
 * an island can end up in another district), and moving them changes no population or vote
 * total, so seat results and the PASS checks are unaffected.
 */
public final class EmptyTractCleanup {
    
    private EmptyTractCleanup() {
    }
    
    /**
     * Reassigns empty tracts in place; returns how many moved.
     */
    public static int apply(Graph g, int[] assign, int k) {
        int n = g.size();
        int[] count = new int[k];
        for (int d : assign) {
            count[d]++;
        }
        double[] shared = new double[k];
        int moved = 0;
        for (int pass = 0; pass < 3; pass++) {
            int movedThisPass = 0;
            for (int u = 0; u < n; u++) {
                if (g.pop(u) != 0 || g.dem(u) + g.rep(u) != 0) {
                    continue;
                }
                int a = assign[u];
                if (count[a] <= 1) {
                    continue;
                }
                int[] nb = g.neighborsOf(u);
                double[] len = g.edgeLengthsOf(u);
                java.util.Arrays.fill(shared, 0);
                for (int j = 0; j < nb.length; j++) {
                    shared[assign[nb[j]]] += len[j];
                }
                int best = a;
                for (int d = 0; d < k; d++) {
                    if (shared[d] > shared[best] + 1e-9) {
                        best = d;
                    }
                }
                if (best == a || !connectedWithout(g, assign, a, u)) {
                    continue;
                }
                assign[u] = best;
                count[a]--;
                count[best]++;
                movedThisPass++;
            }
            moved += movedThisPass;
            if (movedThisPass == 0) {
                break;
            }
        }
        return moved;
    }
    
    private static boolean connectedWithout(Graph g, int[] assign, int d, int skip) {
        int start = -1;
        int size = 0;
        for (int i = 0; i < g.size(); i++) {
            if (assign[i] == d && i != skip) {
                size++;
                if (start < 0) {
                    start = i;
                }
            }
        }
        if (size == 0) {
            return false;
        }
        boolean[] seen = new boolean[g.size()];
        ArrayDeque<Integer> q = new ArrayDeque<>();
        q.add(start);
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
}
