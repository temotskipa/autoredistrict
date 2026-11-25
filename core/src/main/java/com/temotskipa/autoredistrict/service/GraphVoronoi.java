package com.temotskipa.autoredistrict.service;

import com.temotskipa.autoredistrict.engine.Graph;
import com.temotskipa.autoredistrict.engine.InitialMap;

import java.util.Arrays;
import java.util.SplittableRandom;

/**
 * The starting map: a graph Voronoi tessellation. k random seed tracts (with population) are
 * picked, and every tract joins the seed nearest to it by shortest path over centroid-to-centroid
 * distances (multi-source Dijkstra). Each district is a shortest-path tree, so all are contiguous
 * from the start; like the video's random Voronoi start, they are deliberately unbalanced.
 */
public final class GraphVoronoi implements InitialMap {
    
    public static final GraphVoronoi INSTANCE = new GraphVoronoi();
    
    private GraphVoronoi() {
    }
    
    @Override
    public int[] assign(Graph g, int k, SplittableRandom rng) {
        int n = g.size();
        if (k > n) {
            throw new IllegalArgumentException(k + " districts for " + n + " tracts");
        }
        var owner = new int[n];
        Arrays.fill(owner, -1);
        var dist = new double[n];
        Arrays.fill(dist, Double.POSITIVE_INFINITY);
        var heap = new Heap(Math.max(16, 2 * g.edgeCount() + k));
        int populated = 0;
        for (int i = 0; i < n; i++) {
            if (g.pop(i) > 0) {
                populated++;
            }
        }
        boolean needPop = populated >= k;
        int placed = 0;
        while (placed < k) {
            int s = rng.nextInt(n);
            if (owner[s] >= 0 || (needPop && g.pop(s) == 0)) {
                continue;
            }
            owner[s] = placed++;
            dist[s] = 0;
            heap.push(0, s);
        }
        var done = new boolean[n];
        while (!heap.isEmpty()) {
            double d = heap.topKey();
            int u = heap.pop();
            if (done[u] || d > dist[u]) {
                continue;
            }
            done[u] = true;
            int[] nb = g.neighborsOf(u);
            for (int v : nb) {
                double nd = d + Math.hypot(g.x(u) - g.x(v), g.y(u) - g.y(v));
                if (nd < dist[v]) {
                    dist[v] = nd;
                    owner[v] = owner[u];
                    heap.push(nd, v);
                }
            }
        }
        for (int i = 0; i < n; i++) {
            if (owner[i] < 0) {
                throw new IllegalStateException("tract " + i + " is unreachable; the graph isn't connected");
            }
        }
        return owner;
    }
    
    /**
     * Binary min-heap of (distance, unit) with lazy deletion.
     */
    private static final class Heap {
        private double[] key;
        private int[] val;
        private int size;
        
        Heap(int cap) {
            key = new double[cap];
            val = new int[cap];
        }
        
        boolean isEmpty() {
            return size == 0;
        }
        
        double topKey() {
            return key[0];
        }
        
        void push(double k, int v) {
            if (size == key.length) {
                key = Arrays.copyOf(key, size * 2);
                val = Arrays.copyOf(val, size * 2);
            }
            int i = size++;
            while (i > 0) {
                int p = (i - 1) / 2;
                if (key[p] <= k) {
                    break;
                }
                key[i] = key[p];
                val[i] = val[p];
                i = p;
            }
            key[i] = k;
            val[i] = v;
        }
        
        int pop() {
            int top = val[0];
            double k = key[--size];
            int v = val[size];
            int i = 0;
            while (true) {
                int c = 2 * i + 1;
                if (c >= size) {
                    break;
                }
                if (c + 1 < size && key[c + 1] < key[c]) {
                    c++;
                }
                if (key[c] >= k) {
                    break;
                }
                key[i] = key[c];
                val[i] = val[c];
                i = c;
            }
            key[i] = k;
            val[i] = v;
            return top;
        }
    }
}
