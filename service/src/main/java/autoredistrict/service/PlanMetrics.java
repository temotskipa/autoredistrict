package autoredistrict.service;

import autoredistrict.data.StateDataset;
import autoredistrict.data.TractCache;
import autoredistrict.engine.Graph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Metrics of a finished plan, recomputed from scratch from the assignment (not from the engine's
 * tallies), with the checks a PASS needs.
 */
public record PlanMetrics(int districts, List<DistrictResult> rows, double maxDeviation, boolean allContiguous, boolean assignedOnce, boolean populationMatches, long population, long importedPopulation, int demWon, int repWon, double closestMargin) {
    
    public static PlanMetrics compute(StateDataset ds, int[] assignment, int k) {
        Graph g = ds.graph();
        int n = g.size();
        boolean assignedOnce = assignment.length == n;
        long[] pop = new long[k];
        double[] dem = new double[k];
        double[] rep = new double[k];
        double[] area = new double[k];
        double[] perim = new double[k];
        int[] tracts = new int[k];
        TractCache.Tracts t = ds.tracts();
        for (int i = 0; i < Math.min(n, assignment.length); i++) {
            int d = assignment[i];
            if (d < 0 || d >= k) {
                assignedOnce = false;
                continue;
            }
            pop[d] += g.pop(i);
            dem[d] += g.dem(i);
            rep[d] += g.rep(i);
            area[d] += g.area(i);
            perim[d] += t.outer()[i];
            tracts[d]++;
        }
        TractCache.Edges e = ds.edges();
        for (int j = 0; j < e.a().length; j++) {
            int da = assignment[e.a()[j]];
            int db = assignment[e.b()[j]];
            if (da != db) {
                perim[da] += e.length()[j];
                perim[db] += e.length()[j];
            }
        }
        boolean[] contiguous = contiguity(g, assignment, k);
        long total = 0;
        for (long p : pop) {
            total += p;
        }
        double ideal = (double) total / k;
        List<DistrictResult> rows = new ArrayList<>(k);
        double maxDev = 0;
        boolean allContiguous = true;
        int demWon = 0;
        double closest = Double.POSITIVE_INFINITY;
        for (int d = 0; d < k; d++) {
            double dev = (pop[d] - ideal) / ideal;
            maxDev = Math.max(maxDev, Math.abs(dev));
            double two = dem[d] + rep[d];
            double share = two > 0 ? dem[d] / two : 0.5;
            double margin = (2 * share - 1) * 100;
            String winner = share > 0.5 ? "D" : "R";
            if (share > 0.5) {
                demWon++;
            }
            closest = Math.min(closest, Math.abs(margin));
            allContiguous &= contiguous[d] && tracts[d] > 0;
            double pp = perim[d] > 0 ? 4 * Math.PI * area[d] / (perim[d] * perim[d]) : 0;
            rows.add(new DistrictResult(d + 1, pop[d], dev, dem[d], rep[d], share, winner, margin, tracts[d], area[d] / 1e6, pp, contiguous[d]));
        }
        return new PlanMetrics(k, List.copyOf(rows), maxDev, allContiguous, assignedOnce, total == ds.totalPopulation(), total, ds.totalPopulation(), demWon, k - demWon, closest);
    }
    
    /**
     * Full BFS per district over the tract graph (bridges included).
     */
    static boolean[] contiguity(Graph g, int[] assignment, int k) {
        int n = g.size();
        boolean[] seen = new boolean[n];
        int[] components = new int[k];
        ArrayDeque<Integer> q = new ArrayDeque<>();
        for (int s = 0; s < n; s++) {
            if (seen[s]) {
                continue;
            }
            int d = assignment[s];
            components[d]++;
            seen[s] = true;
            q.add(s);
            while (!q.isEmpty()) {
                int u = q.poll();
                for (int v : g.neighborsOf(u)) {
                    if (!seen[v] && assignment[v] == d) {
                        seen[v] = true;
                        q.add(v);
                    }
                }
            }
        }
        boolean[] ok = new boolean[k];
        for (int d = 0; d < k; d++) {
            ok[d] = components[d] == 1;
        }
        return ok;
    }
    
    /**
     * Seats as apportioned, max deviation within tolerance, contiguous, assigned once, population matches.
     */
    public boolean pass(int apportionedSeats, double tolerance) {
        return districts == apportionedSeats && maxDeviation <= tolerance + 1e-12 && allContiguous && assignedOnce && populationMatches;
    }
    
    public double[] demShares() {
        double[] s = new double[rows.size()];
        for (int d = 0; d < s.length; d++) {
            s[d] = rows.get(d).demShare();
        }
        return s;
    }
}
