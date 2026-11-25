package com.temotskipa.autoredistrict.engine;

import java.util.Arrays;
import java.util.SplittableRandom;

/**
 * The mutable state of one Markov chain: an assignment of units to districts plus running
 * tallies per district, so each flip updates two districts in O(degree + k) and never re-adds
 * the map (the video's 20:22 efficiency trick). Everything is primitive arrays and the hot path
 * allocates nothing.
 *
 * <p>Score terms (lower is better), each averaged over the k districts:
 * <ul>
 *   <li><b>population</b>: ((P - ideal) / (tolerance * ideal))^4, the video's fourth-power spread;</li>
 *   <li><b>compactness</b>: moment of inertia about the district's weighted centre, normalised by
 *       weight times area (scale free; about 0.16 for a disc). The weight is population plus a
 *       small area term, so empty tracts aren't free to wander;</li>
 *   <li><b>surface tension</b>: boundary length shared with other districts over the
 *       circumference of a disc of the district's area;</li>
 *   <li><b>election fit</b>: the seat-band penalty ({@link SeatBands}).</li>
 * </ul>
 * Population is also a hard rule: a flip that would push a district that is inside the tolerance
 * outside it is rejected.
 */
public final class Districting {
    
    /**
     * Share of the state's mean density added to the compactness weight per unit of area.
     */
    static final double AREA_WEIGHT = 0.25;
    /**
     * Largest piece a pruning move carries along with the flipped unit.
     */
    static final int MAX_PIECE = 40;
    final int[] assign;
    final long[] pop;
    final int[] count;
    final double[] dem;
    final double[] rep;
    final double[] w;
    final double[] sx;
    final double[] sy;
    final double[] sxx;
    final double[] area;
    final double[] cut;
    final int[] foreign;
    private final Graph g;
    private final int n;
    private final int k;
    private final int[] off;
    private final int[] nbr;
    private final double[] elen;
    private final int[] upop;
    private final double[] ux;
    private final double[] uy;
    private final double[] uw;
    private final double[] uarea;
    private final double[] udem;
    private final double[] urep;
    private final int[] bset;
    private final int[] bpos;
    private final double ideal;
    private final double tolAbs;
    private final double[] popTerm;
    private final double[] compTerm;
    private final double[] tensTerm;
    private final double[] share;
    // Contiguity scratch.
    private final int[] visit;
    private final int[] queue;
    private final int[] lab;
    private final int[] same;
    private final int[] ufp;
    private final int[] act;
    private final int[] grp;
    private final int[] block;
    private final int[] seeds2;
    private final int[] piece = new int[MAX_PIECE];
    private int blockStamp;
    private int sameCount;
    private int pieceSize = -1;
    private int bsize;
    private boolean hardBound = true;
    private double wPop = 1;
    private double wComp;
    private double wTens;
    private double wElec;
    private int demSeats;
    private double hi = 0.5;
    private double lo = 0.5;
    private double popSum;
    private double compSum;
    private double tensSum;
    private double elecPen;
    private double[] sorted;
    private double[] scratch;
    // Pending move from the last evaluate().
    private int pu = -1;
    private int pA;
    private int pB;
    private long pPopA;
    private long pPopB;
    private double pPopTA;
    private double pPopTB;
    private double pCompA;
    private double pCompB;
    private double pTensA;
    private double pTensB;
    private double pCutA;
    private double pCutB;
    private double pShareA;
    private double pShareB;
    private double pElec;
    private boolean pElecValid;
    private int stamp;
    
    public Districting(Graph g, int k, int[] initial, double tolerance) {
        if (initial.length != g.size()) {
            throw new IllegalArgumentException("assignment has " + initial.length + " units, graph " + g.size());
        }
        if (k < 1 || k > g.size()) {
            throw new IllegalArgumentException("bad district count " + k);
        }
        this.g = g;
        this.n = g.size();
        this.k = k;
        this.off = g.offsets();
        this.nbr = g.neighbors();
        this.elen = g.edgeLengths();
        this.upop = g.popArray();
        this.ux = new double[n];
        this.uy = new double[n];
        this.uw = new double[n];
        this.uarea = new double[n];
        this.udem = new double[n];
        this.urep = new double[n];
        double totalArea = g.totalArea();
        double meanPop = (double) g.totalPop() / n;
        double density = totalArea > 0 ? g.totalPop() / totalArea : 0;
        double wsum = 0;
        double cx = 0;
        double cy = 0;
        for (int i = 0; i < n; i++) {
            uarea[i] = g.area(i);
            udem[i] = g.dem(i);
            urep[i] = g.rep(i);
            uw[i] = upop[i] + Math.min(AREA_WEIGHT * density * uarea[i], 2 * meanPop);
            if (uw[i] <= 0) {
                uw[i] = 1e-6 * Math.max(1, meanPop);
            }
            wsum += uw[i];
            cx += uw[i] * g.x(i);
            cy += uw[i] * g.y(i);
        }
        cx /= wsum;
        cy /= wsum;
        for (int i = 0; i < n; i++) {
            ux[i] = g.x(i) - cx;
            uy[i] = g.y(i) - cy;
        }
        this.assign = initial.clone();
        this.pop = new long[k];
        this.count = new int[k];
        this.dem = new double[k];
        this.rep = new double[k];
        this.w = new double[k];
        this.sx = new double[k];
        this.sy = new double[k];
        this.sxx = new double[k];
        this.area = new double[k];
        this.cut = new double[k];
        this.foreign = new int[n];
        this.bset = new int[n];
        this.bpos = new int[n];
        this.ideal = (double) g.totalPop() / k;
        this.tolAbs = tolerance * ideal;
        this.popTerm = new double[k];
        this.compTerm = new double[k];
        this.tensTerm = new double[k];
        this.share = new double[k];
        this.sorted = new double[k];
        this.scratch = new double[k];
        this.visit = new int[n];
        this.queue = new int[n];
        this.lab = new int[n];
        int md = g.maxDegree() + 1;
        this.same = new int[md];
        this.ufp = new int[md];
        this.act = new int[md];
        this.grp = new int[md];
        this.block = new int[n];
        this.seeds2 = new int[md];
        recomputeAll();
    }
    
    // ---------------------------------------------------------------- full recomputation
    
    private static double compOf(double wt, double x, double y, double xx, double a) {
        if (wt <= 0 || a <= 0) {
            return 0;
        }
        double inertia = xx - (x * x + y * y) / wt;
        return inertia <= 0 ? 0 : inertia / (wt * a);
    }
    
    private static double tensOf(double c, double a) {
        return a <= 0 ? 0 : c / (2 * Math.sqrt(Math.PI * a));
    }
    
    private static double shareOf(double d, double r) {
        double t = d + r;
        return t > 0 ? d / t : 0.5;
    }
    
    /**
     * Rebuilds every tally from the assignment.
     */
    void recomputeAll() {
        Arrays.fill(pop, 0);
        Arrays.fill(count, 0);
        Arrays.fill(dem, 0);
        Arrays.fill(rep, 0);
        Arrays.fill(w, 0);
        Arrays.fill(sx, 0);
        Arrays.fill(sy, 0);
        Arrays.fill(sxx, 0);
        Arrays.fill(area, 0);
        Arrays.fill(cut, 0);
        for (int i = 0; i < n; i++) {
            int d = assign[i];
            if (d < 0 || d >= k) {
                throw new IllegalArgumentException("unit " + i + " has district " + d);
            }
            pop[d] += upop[i];
            count[d]++;
            dem[d] += udem[i];
            rep[d] += urep[i];
            w[d] += uw[i];
            sx[d] += uw[i] * ux[i];
            sy[d] += uw[i] * uy[i];
            sxx[d] += uw[i] * (ux[i] * ux[i] + uy[i] * uy[i]);
            area[d] += uarea[i];
        }
        bsize = 0;
        Arrays.fill(bpos, -1);
        for (int i = 0; i < n; i++) {
            int f = 0;
            for (int e = off[i]; e < off[i + 1]; e++) {
                if (assign[nbr[e]] != assign[i]) {
                    f++;
                    cut[assign[i]] += elen[e];
                }
            }
            foreign[i] = f;
            if (f > 0) {
                bAdd(i);
            }
        }
        recomputeTerms();
    }
    
    /**
     * Recomputes the per-district terms and their sums from the tallies.
     */
    void recomputeTerms() {
        popSum = 0;
        compSum = 0;
        tensSum = 0;
        for (int d = 0; d < k; d++) {
            popTerm[d] = popTermOf(pop[d]);
            compTerm[d] = compOf(w[d], sx[d], sy[d], sxx[d], area[d]);
            tensTerm[d] = tensOf(cut[d], area[d]);
            popSum += popTerm[d];
            compSum += compTerm[d];
            tensSum += tensTerm[d];
            share[d] = shareOf(dem[d], rep[d]);
        }
        System.arraycopy(share, 0, sorted, 0, k);
        Arrays.sort(sorted);
        elecPen = SeatBands.penaltySorted(sorted, k, demSeats, hi, lo);
    }
    
    private double popTermOf(long p) {
        double z = (p - ideal) / tolAbs;
        z *= z;
        return z * z;
    }
    
    // ---------------------------------------------------------------- configuration
    
    /**
     * Turns the hard population rule on or off (on by default).
     */
    public void setHardBound(boolean on) {
        this.hardBound = on;
    }
    
    public void setWeights(Weights weights) {
        this.wPop = weights.population();
        this.wComp = weights.compactness();
        this.wTens = weights.tension();
        this.wElec = weights.election();
    }
    
    public void setTarget(int demSeats, double marginPoints) {
        if (demSeats < 0 || demSeats > k) {
            throw new IllegalArgumentException("dem seats " + demSeats + " outside 0.." + k);
        }
        this.demSeats = demSeats;
        this.hi = SeatBands.hi(marginPoints);
        this.lo = SeatBands.lo(marginPoints);
        recomputeTerms();
    }
    
    // ---------------------------------------------------------------- moves
    
    /**
     * Picks a boundary unit. With {@code bias} > 0, a unit is kept with probability
     * (foreign neighbours / degree)^bias, so units with more foreign neighbours are chosen more
     * often (the video's statistical surface tension). Returns -1 if there is no boundary.
     */
    int pickBoundary(SplittableRandom rng, double bias) {
        if (bsize == 0) {
            return -1;
        }
        for (int attempt = 0; attempt < 32; attempt++) {
            int u = bset[rng.nextInt(bsize)];
            if (bias <= 0) {
                return u;
            }
            double frac = (double) foreign[u] / (off[u + 1] - off[u]);
            if (rng.nextDouble() < Math.pow(frac, bias)) {
                return u;
            }
        }
        return bset[rng.nextInt(bsize)];
    }
    
    /**
     * A random foreign neighbour's district for boundary unit {@code u}.
     */
    int pickTarget(int u, SplittableRandom rng) {
        int a = assign[u];
        int j = rng.nextInt(foreign[u]);
        for (int e = off[u]; e < off[u + 1]; e++) {
            int d = assign[nbr[e]];
            if (d != a && j-- == 0) {
                return d;
            }
        }
        throw new IllegalStateException("foreign count out of sync at unit " + u);
    }
    
    /**
     * Scores moving {@code u} to district {@code b}. Returns the change in total score, or NaN if
     * the move is forbidden (it would empty a district or break the population bound). The move
     * is remembered for {@link #commit()}.
     */
    double evaluate(int u, int b) {
        int a = assign[u];
        pu = -1;
        if (count[a] <= 1) {
            return Double.NaN;
        }
        int pu0 = upop[u];
        long na = pop[a] - pu0;
        long nb = pop[b] + pu0;
        if (hardBound && inside(pop[a]) && !inside(na)) {
            return Double.NaN;
        }
        if (hardBound && inside(pop[b]) && !inside(nb)) {
            return Double.NaN;
        }
        double la = 0;
        double lb = 0;
        double lt = 0;
        for (int e = off[u]; e < off[u + 1]; e++) {
            double len = elen[e];
            lt += len;
            int d = assign[nbr[e]];
            if (d == a) {
                la += len;
            }
            else if (d == b) {
                lb += len;
            }
        }
        double wu = uw[u];
        double xu = ux[u];
        double yu = uy[u];
        double ru2 = xu * xu + yu * yu;
        double au = uarea[u];
        double areaA = area[a] - au;
        double areaB = area[b] + au;
        pPopA = na;
        pPopB = nb;
        pPopTA = popTermOf(na);
        pPopTB = popTermOf(nb);
        pCompA = compOf(w[a] - wu, sx[a] - wu * xu, sy[a] - wu * yu, sxx[a] - wu * ru2, areaA);
        pCompB = compOf(w[b] + wu, sx[b] + wu * xu, sy[b] + wu * yu, sxx[b] + wu * ru2, areaB);
        pCutA = cut[a] + 2 * la - lt;
        pCutB = cut[b] + lt - 2 * lb;
        pTensA = tensOf(pCutA, areaA);
        pTensB = tensOf(pCutB, areaB);
        pShareA = shareOf(dem[a] - udem[u], rep[a] - urep[u]);
        pShareB = shareOf(dem[b] + udem[u], rep[b] + urep[u]);
        double delta = wPop * (pPopTA + pPopTB - popTerm[a] - popTerm[b]) + wComp * (pCompA + pCompB - compTerm[a] - compTerm[b]) + wTens * (pTensA + pTensB - tensTerm[a] - tensTerm[b]);
        if (wElec != 0) {
            SeatBands.replaceTwo(sorted, k, share[a], share[b], pShareA, pShareB, scratch);
            pElec = SeatBands.penaltySorted(scratch, k, demSeats, hi, lo);
            pElecValid = true;
            delta += wElec * (pElec - elecPen);
        }
        else {
            pElecValid = false;
        }
        pu = u;
        pA = a;
        pB = b;
        return delta / k;
    }
    
    /**
     * Applies the move from the last successful {@link #evaluate(int, int)}.
     */
    void commit() {
        int u = pu;
        if (u < 0) {
            throw new IllegalStateException("no pending move");
        }
        int a = pA;
        int b = pB;
        assign[u] = b;
        pop[a] = pPopA;
        pop[b] = pPopB;
        count[a]--;
        count[b]++;
        dem[a] -= udem[u];
        rep[a] -= urep[u];
        dem[b] += udem[u];
        rep[b] += urep[u];
        double wu = uw[u];
        double xu = ux[u];
        double yu = uy[u];
        double ru2 = xu * xu + yu * yu;
        w[a] -= wu;
        w[b] += wu;
        sx[a] -= wu * xu;
        sx[b] += wu * xu;
        sy[a] -= wu * yu;
        sy[b] += wu * yu;
        sxx[a] -= wu * ru2;
        sxx[b] += wu * ru2;
        area[a] -= uarea[u];
        area[b] += uarea[u];
        cut[a] = pCutA;
        cut[b] = pCutB;
        int fu = 0;
        for (int e = off[u]; e < off[u + 1]; e++) {
            int v = nbr[e];
            int dv = assign[v];
            if (dv == a) {
                if (++foreign[v] == 1) {
                    bAdd(v);
                }
                fu++;
            }
            else if (dv == b) {
                if (--foreign[v] == 0) {
                    bRemove(v);
                }
            }
            else {
                fu++;
            }
        }
        foreign[u] = fu;
        if (fu > 0) {
            bAdd(u);
        }
        else {
            bRemove(u);
        }
        popSum += pPopTA + pPopTB - popTerm[a] - popTerm[b];
        compSum += pCompA + pCompB - compTerm[a] - compTerm[b];
        tensSum += pTensA + pTensB - tensTerm[a] - tensTerm[b];
        popTerm[a] = pPopTA;
        popTerm[b] = pPopTB;
        compTerm[a] = pCompA;
        compTerm[b] = pCompB;
        tensTerm[a] = pTensA;
        tensTerm[b] = pTensB;
        if (!pElecValid) {
            SeatBands.replaceTwo(sorted, k, share[a], share[b], pShareA, pShareB, scratch);
            pElec = SeatBands.penaltySorted(scratch, k, demSeats, hi, lo);
        }
        double[] t = sorted;
        sorted = scratch;
        scratch = t;
        share[a] = pShareA;
        share[b] = pShareB;
        elecPen = pElec;
        pu = -1;
    }
    
    private boolean inside(long p) {
        return Math.abs(p - ideal) <= tolAbs;
    }
    
    // ---------------------------------------------------------------- contiguity
    
    /**
     * Whether district {@code assign[u]} stays connected without {@code u}. Fast path: the moved
     * unit's same-district neighbours are connected among themselves. Otherwise a simultaneous
     * BFS from each of them inside the district, stopping as soon as all have met or one search
     * runs out (which finds the smaller side of a split quickly).
     */
    boolean removalKeepsConnected(int u) {
        if (neighboursMeetLocally(u)) {
            return true;
        }
        nextBlock();
        block[u] = blockStamp;
        return seedsConnect(assign[u], same, sameCount);
    }
    
    /**
     * Whether removing {@code u} cuts a piece of at most {@link #MAX_PIECE} units off its district,
     * kept in {@link #piece} for {@link #tryPieceMove}. Like {@link #removalKeepsConnected} it
     * searches from each same-district neighbour at once, but a search stops growing once it holds
     * more than {@code MAX_PIECE} units, so the cost is bounded by the unit's degree however large
     * the district. The first search to run out within the limit is the piece.
     */
    boolean cutsOffSmallPiece(int u) {
        if (neighboursMeetLocally(u)) {
            return false;
        }
        int a = assign[u];
        int m = sameCount;
        nextBlock();
        block[u] = blockStamp;
        nextStamp();
        int head = 0;
        int tail = 0;
        for (int i = 0; i < m; i++) {
            int s = same[i];
            visit[s] = stamp;
            lab[s] = i;
            ufp[i] = i;
            act[i] = 1;
            grp[i] = 1;
            queue[tail++] = s;
        }
        int groups = m;
        while (head < tail) {
            int x = queue[head++];
            int r = find(lab[x]);
            act[r]--;
            if (grp[r] > MAX_PIECE) {
                continue; // too big to be the piece: stop growing it
            }
            for (int e = off[x]; e < off[x + 1]; e++) {
                int v = nbr[e];
                if (block[v] == blockStamp || assign[v] != a) {
                    continue;
                }
                if (visit[v] != stamp) {
                    visit[v] = stamp;
                    lab[v] = r;
                    queue[tail++] = v;
                    act[r]++;
                    grp[r]++;
                }
                else {
                    int r2 = find(lab[v]);
                    if (r2 != r) {
                        ufp[r2] = r;
                        act[r] += act[r2];
                        grp[r] += grp[r2];
                        if (--groups == 1) {
                            return false;
                        }
                    }
                }
            }
            if (act[r] == 0 && grp[r] <= MAX_PIECE) {
                collectPiece(r, tail);
                return pieceSize > 0;
            }
        }
        return false;
    }
    
    /**
     * Collects {@code u}'s same-district neighbours in {@link #same} and returns true if there is
     * at most one or they are connected through each other, the cheap case in which removing
     * {@code u} can't split the district.
     */
    private boolean neighboursMeetLocally(int u) {
        int a = assign[u];
        int m = 0;
        for (int e = off[u]; e < off[u + 1]; e++) {
            int v = nbr[e];
            if (assign[v] == a) {
                same[m++] = v;
            }
        }
        sameCount = m;
        pieceSize = -1;
        if (m <= 1) {
            return true;
        }
        nextStamp();
        for (int i = 0; i < m; i++) {
            visit[same[i]] = stamp;
            lab[same[i]] = i;
            ufp[i] = i;
        }
        int groups = m;
        for (int i = 0; i < m && groups > 1; i++) {
            int s = same[i];
            for (int e = off[s]; e < off[s + 1]; e++) {
                int v = nbr[e];
                if (visit[v] == stamp) {
                    int r1 = find(i);
                    int r2 = find(lab[v]);
                    if (r1 != r2) {
                        ufp[r2] = r1;
                        groups--;
                    }
                }
            }
        }
        return groups == 1;
    }
    
    /**
     * Simultaneous BFS inside district {@code a} from each seed, skipping blocked units. Returns
     * true once all seeds have met, false as soon as one search runs out.
     */
    private boolean seedsConnect(int a, int[] seeds, int m) {
        nextStamp();
        int head = 0;
        int tail = 0;
        for (int i = 0; i < m; i++) {
            int s = seeds[i];
            visit[s] = stamp;
            lab[s] = i;
            ufp[i] = i;
            act[i] = 1;
            queue[tail++] = s;
        }
        int groups = m;
        while (head < tail) {
            int x = queue[head++];
            int r = find(lab[x]);
            act[r]--;
            for (int e = off[x]; e < off[x + 1]; e++) {
                int v = nbr[e];
                if (block[v] == blockStamp || assign[v] != a) {
                    continue;
                }
                if (visit[v] != stamp) {
                    visit[v] = stamp;
                    lab[v] = r;
                    queue[tail++] = v;
                    act[r]++;
                }
                else {
                    int r2 = find(lab[v]);
                    if (r2 != r) {
                        ufp[r2] = r;
                        act[r] += act[r2];
                        if (--groups == 1) {
                            return true;
                        }
                    }
                }
            }
            if (act[r] == 0) {
                return false;
            }
        }
        return groups == 1;
    }
    
    private void collectPiece(int root, int tail) {
        int c = 0;
        for (int i = 0; i < tail; i++) {
            int x = queue[i];
            if (find(lab[x]) == root) {
                if (c == MAX_PIECE) {
                    pieceSize = -1;
                    return;
                }
                piece[c++] = x;
            }
        }
        pieceSize = c;
    }
    
    /**
     * Blocks {@code u} and the piece it cuts off, the units of a pruning move.
     */
    private void blockPieceMove(int u) {
        nextBlock();
        block[u] = blockStamp;
        for (int i = 0; i < pieceSize; i++) {
            block[piece[i]] = blockStamp;
        }
    }
    
    /**
     * After {@link #blockPieceMove(int)}: whether the rest of the district (without {@code u} and
     * the small piece it cuts off) is still connected.
     */
    private boolean pieceRemainderConnected(int u) {
        int m = 0;
        for (int i = 0; i < sameCount; i++) {
            if (block[same[i]] != blockStamp) {
                seeds2[m++] = same[i];
            }
        }
        if (m == 0) {
            return false;
        }
        return m == 1 || seedsConnect(assign[u], seeds2, m);
    }
    
    /**
     * The pruning move: when flipping {@code u} would cut a small piece (at most
     * {@link #MAX_PIECE} units) off its district, move {@code u} and that piece to {@code b}
     * together. One Metropolis test at temperature {@code t} on the whole move's score change
     * ({@link #pieceDelta}) decides; an accepted move is applied as exact single-unit updates.
     * Without it a district whose whole boundary consists of cut vertices (a one-tract-wide tail)
     * can never shrink. Call right after {@code cutsOffSmallPiece(u)} returned true.
     */
    boolean tryPieceMove(int u, int b, double t, SplittableRandom rng) {
        double total = pieceDelta(u, b);
        if (Double.isNaN(total) || (total > 0 && rng.nextDouble() >= Math.exp(-total / t))) {
            return false;
        }
        // Checked after the test, like a single flip's contiguity: most moves are rejected first.
        if (!pieceRemainderConnected(u)) {
            return false;
        }
        int cnt = pieceSize;
        boolean hb = hardBound;
        hardBound = false; // the bound applies to the whole move, which pieceDelta checked
        try {
            for (int i = -1; i < cnt; i++) {
                if (Double.isNaN(evaluate(i < 0 ? u : piece[i], b))) {
                    throw new IllegalStateException("could not apply a piece move at unit " + (i < 0 ? u : piece[i]));
                }
                commit();
            }
        } finally {
            hardBound = hb;
        }
        return true;
    }
    
    /**
     * The score change of moving {@code u} and the piece it cuts off to {@code b} together,
     * computed from the district tallies without changing them (every term is a function of
     * per-district sums). NaN if the move would push a district that is inside the population
     * bound outside it; whether the rest of the district stays connected isn't checked here.
     * Call right after {@code cutsOffSmallPiece(u)} returned true.
     */
    double pieceDelta(int u, int b) {
        pu = -1; // scratch is reused below, so no single move stays pending
        if (pieceSize <= 0) {
            return Double.NaN;
        }
        blockPieceMove(u); // the blocked units are the move
        int a = assign[u];
        long dp = 0;
        double dw = 0;
        double dsx = 0;
        double dsy = 0;
        double dsxx = 0;
        double dar = 0;
        double dd = 0;
        double dr = 0;
        double la = 0;
        double lb = 0;
        double lt = 0;
        for (int i = -1; i < pieceSize; i++) {
            int s = i < 0 ? u : piece[i];
            double ws = uw[s];
            double xs = ux[s];
            double ys = uy[s];
            dp += upop[s];
            dw += ws;
            dsx += ws * xs;
            dsy += ws * ys;
            dsxx += ws * (xs * xs + ys * ys);
            dar += uarea[s];
            dd += udem[s];
            dr += urep[s];
            for (int e = off[s]; e < off[s + 1]; e++) {
                int v = nbr[e];
                if (block[v] == blockStamp) {
                    continue; // moves too: the edge stays inside one district
                }
                double len = elen[e];
                lt += len;
                int dv = assign[v];
                if (dv == a) {
                    la += len;
                }
                else if (dv == b) {
                    lb += len;
                }
            }
        }
        long na = pop[a] - dp;
        long nb = pop[b] + dp;
        if (hardBound && ((inside(pop[a]) && !inside(na)) || (inside(pop[b]) && !inside(nb)))) {
            return Double.NaN;
        }
        double areaA = area[a] - dar;
        double areaB = area[b] + dar;
        double popA = popTermOf(na);
        double popB = popTermOf(nb);
        double compA = compOf(w[a] - dw, sx[a] - dsx, sy[a] - dsy, sxx[a] - dsxx, areaA);
        double compB = compOf(w[b] + dw, sx[b] + dsx, sy[b] + dsy, sxx[b] + dsxx, areaB);
        double tensA = tensOf(cut[a] + 2 * la - lt, areaA);
        double tensB = tensOf(cut[b] + lt - 2 * lb, areaB);
        double delta = wPop * (popA + popB - popTerm[a] - popTerm[b]) + wComp * (compA + compB - compTerm[a] - compTerm[b]) + wTens * (tensA + tensB - tensTerm[a] - tensTerm[b]);
        if (wElec != 0) {
            SeatBands.replaceTwo(sorted, k, share[a], share[b], shareOf(dem[a] - dd, rep[a] - dr), shareOf(dem[b] + dd, rep[b] + dr), scratch);
            delta += wElec * (SeatBands.penaltySorted(scratch, k, demSeats, hi, lo) - elecPen);
        }
        return delta / k;
    }
    
    private void nextBlock() {
        if (++blockStamp == Integer.MAX_VALUE) {
            Arrays.fill(block, 0);
            blockStamp = 1;
        }
    }
    
    private int find(int i) {
        while (ufp[i] != i) {
            ufp[i] = ufp[ufp[i]];
            i = ufp[i];
        }
        return i;
    }
    
    private void nextStamp() {
        if (++stamp == Integer.MAX_VALUE) {
            Arrays.fill(visit, 0);
            stamp = 1;
        }
    }
    
    // ---------------------------------------------------------------- boundary set
    
    private void bAdd(int u) {
        if (bpos[u] < 0) {
            bpos[u] = bsize;
            bset[bsize++] = u;
        }
    }
    
    private void bRemove(int u) {
        int p = bpos[u];
        if (p >= 0) {
            int last = bset[--bsize];
            bset[p] = last;
            bpos[last] = p;
            bpos[u] = -1;
        }
    }
    
    // ---------------------------------------------------------------- reporting
    
    /**
     * Total weighted score (lower is better).
     */
    public double score() {
        return (wPop * popSum + wComp * compSum + wTens * tensSum + wElec * elecPen) / k;
    }
    
    public double populationTerm() {
        return popSum / k;
    }
    
    public double compactnessTerm() {
        return compSum / k;
    }
    
    public double tensionTerm() {
        return tensSum / k;
    }
    
    public double electionPenalty() {
        return elecPen;
    }
    
    public int districts() {
        return k;
    }
    
    public int[] assignment() {
        return assign.clone();
    }
    
    /**
     * Districts with a D share above 50%.
     */
    public int demWon() {
        int c = 0;
        for (int d = 0; d < k; d++) {
            if (share[d] > 0.5) {
                c++;
            }
        }
        return c;
    }
    
    /**
     * Smallest |D - R| / (D + R) over districts, in percentage points.
     */
    public double closestMargin() {
        double best = Double.POSITIVE_INFINITY;
        for (int d = 0; d < k; d++) {
            best = Math.min(best, Math.abs(2 * share[d] - 1) * 100);
        }
        return best;
    }
    
    /**
     * Largest |P - ideal| / ideal.
     */
    public double maxDeviation() {
        double m = 0;
        for (int d = 0; d < k; d++) {
            m = Math.max(m, Math.abs(pop[d] - ideal) / ideal);
        }
        return m;
    }
    
    public boolean allInside() {
        for (int d = 0; d < k; d++) {
            if (!inside(pop[d])) {
                return false;
            }
        }
        return true;
    }
    
    public Graph graph() {
        return g;
    }
    
    /**
     * Checks the running tallies against a full recomputation (the video's "phantom voters"
     * bug) and throws if they drifted beyond rounding.
     */
    public void verifyTallies() {
        long[] p0 = pop.clone();
        int[] c0 = count.clone();
        double[][] d0 = {dem.clone(), rep.clone(), w.clone(), sx.clone(), sy.clone(), sxx.clone(), area.clone(), cut.clone()};
        int[] f0 = foreign.clone();
        int bs0 = bsize;
        double s0 = score();
        recomputeAll();
        if (!Arrays.equals(p0, pop) || !Arrays.equals(c0, count) || !Arrays.equals(f0, foreign) || bs0 != bsize) {
            throw new IllegalStateException("integer tallies drifted from a full recomputation");
        }
        double[][] d1 = {dem, rep, w, sx, sy, sxx, area, cut};
        for (int t = 0; t < d0.length; t++) {
            for (int d = 0; d < k; d++) {
                double scale = Math.max(1, Math.abs(d1[t][d]));
                if (Math.abs(d0[t][d] - d1[t][d]) > 1e-6 * scale) {
                    throw new IllegalStateException("tally " + t + " of district " + d + " drifted: " + d0[t][d] + " vs " + d1[t][d]);
                }
            }
        }
        if (Math.abs(s0 - score()) > 1e-6 * Math.max(1, Math.abs(score()))) {
            throw new IllegalStateException("score drifted: " + s0 + " vs " + score());
        }
    }
    
    // Test hook.
    
    long populationOf(int d) {
        return pop[d];
    }
}
