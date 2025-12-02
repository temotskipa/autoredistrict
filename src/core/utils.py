import numpy as np
import pandas as pd

def ensure_numeric(df: pd.DataFrame, columns):
    """Coerce columns to numeric, filling NaN with 0."""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df


def weighted_partisan_share(gdf):
    """Population-weighted partisan share."""
    if gdf.empty or 'partisan_score' not in gdf.columns:
        return 0.5
    weights = pd.to_numeric(gdf['P1_001N'], errors='coerce').fillna(0)
    shares = pd.to_numeric(gdf['partisan_score'], errors='coerce').fillna(0.5)
    total = weights.sum()
    if total <= 0:
        return shares.mean() if not shares.empty else 0.5
    return float((shares * weights).sum() / total)


def polsby_popper(gdf):
    """Polsby-Popper compactness score."""
    if gdf.empty or gdf.unary_union.area == 0:
        return 0
    perimeter = gdf.unary_union.length
    area = gdf.unary_union.area
    if perimeter == 0:
        return 0
    return (4 * np.pi * area) / (perimeter ** 2)


def build_adjacency(gdf):
    """Queen contiguity adjacency using spatial index."""
    adjacency = {i: set() for i in range(len(gdf))}
    sindex = gdf.sindex
    for i, geom in enumerate(gdf.geometry):
        possible = list(sindex.query(geom, predicate="intersects"))
        for j in possible:
            if i == j:
                continue
            adjacency[i].add(j)
            adjacency[j].add(i)
    return adjacency


def is_contiguous(gdf):
    """Return True if GeoDataFrame is spatially contiguous."""
    if gdf.empty:
        return True
    gdf = gdf.reset_index(drop=True)
    sindex = gdf.sindex
    adj = {i: set() for i in range(len(gdf))}
    for i, geom in enumerate(gdf.geometry):
        hits = sindex.query(geom, predicate="touches")
        for j in hits:
            if i == j:
                continue
            adj[i].add(j)
            adj[j].add(i)
    seen = {0}
    stack = [0]
    while stack:
        cur = stack.pop()
        for nb in adj[cur]:
            if nb not in seen:
                seen.add(nb)
                stack.append(nb)
    return len(seen) == len(gdf)
