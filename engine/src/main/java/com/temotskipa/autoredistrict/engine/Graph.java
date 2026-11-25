package com.temotskipa.autoredistrict.engine;

import java.util.Arrays;
import java.util.Objects;

/**
 * The unit graph the engine works on: one node per tract, with population, votes, area and the
 * centroid in an equal-area projection (metres), and a symmetric CSR adjacency whose edges carry
 * the shared boundary length. Bridge edges added to connect islands have length 0.
 *
 * <p>Immutable. Arrays are owned by the graph; accessors return them directly for speed, so
 * callers must not modify them.
 */
public final class Graph {
    
    private final int n;
    private final int[] pop;
    private final double[] dem;
    private final double[] rep;
    private final double[] x;
    private final double[] y;
    private final double[] area;
    private final int[] offsets;
    private final int[] neighbors;
    private final double[] edgeLength;
    private final long totalPop;
    private final double totalArea;
    private final int maxDegree;
    
    private Graph(int[] pop, double[] dem, double[] rep, double[] x, double[] y, double[] area, int[] offsets, int[] neighbors, double[] edgeLength) {
        this.n = pop.length;
        this.pop = pop;
        this.dem = dem;
        this.rep = rep;
        this.x = x;
        this.y = y;
        this.area = area;
        this.offsets = offsets;
        this.neighbors = neighbors;
        this.edgeLength = edgeLength;
        long p = 0;
        double a = 0;
        int md = 0;
        for (int i = 0; i < n; i++) {
            p += pop[i];
            a += area[i];
            md = Math.max(md, offsets[i + 1] - offsets[i]);
        }
        this.totalPop = p;
        this.totalArea = a;
        this.maxDegree = md;
    }
    
    /**
     * Builds a graph from per-unit arrays and an undirected edge list. Duplicate edges are merged
     * (lengths summed) and self loops are rejected.
     */
    public static Graph of(int[] pop, double[] dem, double[] rep, double[] x, double[] y, double[] area, int[] edgeA, int[] edgeB, double[] edgeLen) {
        int n = pop.length;
        for (double[] arr : new double[][]{dem, rep, x, y, area}) {
            if (arr.length != n) {
                throw new IllegalArgumentException("per-unit arrays must all have length " + n);
            }
        }
        if (edgeA.length != edgeB.length || edgeA.length != edgeLen.length) {
            throw new IllegalArgumentException("edge arrays differ in length");
        }
        int m = edgeA.length;
        var degree = new int[n];
        for (int e = 0; e < m; e++) {
            int a = edgeA[e];
            int b = edgeB[e];
            Objects.checkIndex(a, n);
            Objects.checkIndex(b, n);
            if (a == b) {
                throw new IllegalArgumentException("self loop at unit " + a);
            }
            degree[a]++;
            degree[b]++;
        }
        var offsets = new int[n + 1];
        for (int i = 0; i < n; i++) {
            offsets[i + 1] = offsets[i] + degree[i];
        }
        int[] fill = Arrays.copyOf(offsets, n);
        var nbr = new int[offsets[n]];
        var len = new double[offsets[n]];
        for (int e = 0; e < m; e++) {
            int a = edgeA[e];
            int b = edgeB[e];
            nbr[fill[a]] = b;
            len[fill[a]++] = edgeLen[e];
            nbr[fill[b]] = a;
            len[fill[b]++] = edgeLen[e];
        }
        // Sort each adjacency list and merge duplicates.
        var outOff = new int[n + 1];
        int w = 0;
        for (int i = 0; i < n; i++) {
            int s = offsets[i];
            int t = offsets[i + 1];
            sortPairs(nbr, len, s, t);
            outOff[i] = w;
            for (int j = s; j < t; j++) {
                if (w > outOff[i] && nbr[w - 1] == nbr[j]) {
                    len[w - 1] += len[j];
                }
                else {
                    nbr[w] = nbr[j];
                    len[w] = len[j];
                    w++;
                }
            }
        }
        outOff[n] = w;
        return new Graph(pop.clone(), dem.clone(), rep.clone(), x.clone(), y.clone(), area.clone(), outOff, Arrays.copyOf(nbr, w), Arrays.copyOf(len, w));
    }
    
    private static void sortPairs(int[] keys, double[] vals, int from, int to) {
        for (int i = from + 1; i < to; i++) {
            int k = keys[i];
            double v = vals[i];
            int j = i - 1;
            while (j >= from && keys[j] > k) {
                keys[j + 1] = keys[j];
                vals[j + 1] = vals[j];
                j--;
            }
            keys[j + 1] = k;
            vals[j + 1] = v;
        }
    }
    
    public int size() {
        return n;
    }
    
    public int pop(int i) {
        return pop[i];
    }
    
    public double dem(int i) {
        return dem[i];
    }
    
    public double rep(int i) {
        return rep[i];
    }
    
    public double x(int i) {
        return x[i];
    }
    
    public double y(int i) {
        return y[i];
    }
    
    public double area(int i) {
        return area[i];
    }
    
    public int maxDegree() {
        return maxDegree;
    }
    
    public long totalPop() {
        return totalPop;
    }
    
    public double totalArea() {
        return totalArea;
    }
    
    /**
     * Number of undirected edges.
     */
    public int edgeCount() {
        return neighbors.length / 2;
    }
    
    // Raw CSR access for the hot loop.
    
    int[] offsets() {
        return offsets;
    }
    
    int[] neighbors() {
        return neighbors;
    }
    
    double[] edgeLengths() {
        return edgeLength;
    }
    
    int[] popArray() {
        return pop;
    }
    
    /**
     * Neighbour list of unit {@code i} as a fresh array (not for hot loops).
     */
    public int[] neighborsOf(int i) {
        return Arrays.copyOfRange(neighbors, offsets[i], offsets[i + 1]);
    }
    
    /**
     * Shared boundary lengths parallel to {@link #neighborsOf(int)}.
     */
    public double[] edgeLengthsOf(int i) {
        return Arrays.copyOfRange(edgeLength, offsets[i], offsets[i + 1]);
    }
}
