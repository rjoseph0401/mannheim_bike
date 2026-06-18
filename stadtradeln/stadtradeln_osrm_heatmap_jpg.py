from pathlib import Path
import json
from collections import Counter

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import osmnx as ox
import pandas as pd
from matplotlib import cm
from matplotlib.colors import LogNorm
from shapely.geometry import Point, box

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
CACHE_DIR = DATA_DIR / "cache"
GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"
GRAPH_FILE_EXPANDED = DATA_DIR / "stadtradeln_bike_expanded.graphml"
ROUTE_CACHE_FILE = CACHE_DIR / "stadtradeln_osrm_routes.json"

YEARS = [2022, 2023, 2024]
MAX_SNAP_DISTANCE_M = 20
GRAPH_BUFFER_M = 1500


def key_of(slon, slat, elon, elat):
    return f"{slon},{slat};{elon},{elat}"


def load_pairs(year):
    parquet = CACHE_DIR / f"stadtradeln_{year}.parquet"
    df = pd.read_parquet(parquet)
    req = {"slon", "slat", "elon", "elat", "number_of_matched_trips"}
    if not req.issubset(df.columns):
        raise ValueError(f"Fehlende Spalten in {parquet.name}")
    pairs = df.groupby(["slon", "slat", "elon", "elat"], as_index=False)["number_of_matched_trips"].sum()
    return pairs[(pairs.slon != pairs.elon) | (pairs.slat != pairs.elat)].copy()


def filter_pairs_in_graph_area(pairs, edges):
    edges_proj = edges.to_crs("EPSG:3857")
    minx, miny, maxx, maxy = edges_proj.total_bounds
    minx -= MAX_SNAP_DISTANCE_M
    miny -= MAX_SNAP_DISTANCE_M
    maxx += MAX_SNAP_DISTANCE_M
    maxy += MAX_SNAP_DISTANCE_M

    start_pts = gpd.GeoSeries(gpd.points_from_xy(pairs.slon, pairs.slat), crs="EPSG:4326").to_crs("EPSG:3857")
    end_pts = gpd.GeoSeries(gpd.points_from_xy(pairs.elon, pairs.elat), crs="EPSG:4326").to_crs("EPSG:3857")
    mask = (
        (start_pts.x >= minx)
        & (start_pts.x <= maxx)
        & (start_pts.y >= miny)
        & (start_pts.y <= maxy)
        & (end_pts.x >= minx)
        & (end_pts.x <= maxx)
        & (end_pts.y >= miny)
        & (end_pts.y <= maxy)
    )
    return pairs[mask].copy(), int((~mask).sum())


def buffered_bounds_wgs84(min_lon, min_lat, max_lon, max_lat, buffer_m):
    pts = gpd.GeoSeries([Point(min_lon, min_lat), Point(max_lon, max_lat)], crs="EPSG:4326").to_crs("EPSG:3857")
    minx, miny, maxx, maxy = pts.total_bounds
    bbox_3857 = box(minx - buffer_m, miny - buffer_m, maxx + buffer_m, maxy + buffer_m)
    return gpd.GeoSeries([bbox_3857], crs="EPSG:3857").to_crs("EPSG:4326").iloc[0].bounds


def load_or_build_expanded_graph(route_cache, pairs_by_year):
    if GRAPH_FILE_EXPANDED.exists():
        print(f"Nutze erweiterten Graph: {GRAPH_FILE_EXPANDED}")
        return ox.load_graphml(GRAPH_FILE_EXPANDED)

    lons, lats = [], []
    for pairs in pairs_by_year.values():
        lons.extend(pairs["slon"].tolist())
        lons.extend(pairs["elon"].tolist())
        lats.extend(pairs["slat"].tolist())
        lats.extend(pairs["elat"].tolist())

        for r in pairs.itertuples():
            coords = route_cache.get(key_of(r.slon, r.slat, r.elon, r.elat))
            if not coords:
                continue
            for lon, lat in coords:
                lons.append(float(lon))
                lats.append(float(lat))

    if not lons or not lats:
        raise RuntimeError("Konnte kein gültiges Stadtradeln-Gebiet aus den Routen bestimmen.")

    min_lon, max_lon = float(np.min(lons)), float(np.max(lons))
    min_lat, max_lat = float(np.min(lats)), float(np.max(lats))
    min_lon, min_lat, max_lon, max_lat = buffered_bounds_wgs84(min_lon, min_lat, max_lon, max_lat, GRAPH_BUFFER_M)
    print(
        "Erzeuge erweiterten Graph für Stadtradeln-Gebiet:",
        f"lon[{min_lon:.5f}, {max_lon:.5f}] lat[{min_lat:.5f}, {max_lat:.5f}]",
    )
    graph = ox.graph_from_polygon(box(min_lon, min_lat, max_lon, max_lat), network_type="bike", simplify=True)
    ox.save_graphml(graph, GRAPH_FILE_EXPANDED)
    return graph


def edge_hits_for_year(graph, edges, route_cache, year, pairs):
    pairs, dropped = filter_pairs_in_graph_area(pairs, edges)
    print(f"{year}: außerhalb Graphgebiet verworfen: {dropped}")

    trips_map = {key_of(r.slon, r.slat, r.elon, r.elat): int(r.number_of_matched_trips) for r in pairs.itertuples()}
    all_x, all_y, route_keys = [], [], []
    for k, trips in trips_map.items():
        coords = route_cache.get(k)
        if not coords or len(coords) < 2:
            continue
        for a, b in zip(coords, coords[1:]):
            all_x.append((a[0] + b[0]) / 2)
            all_y.append((a[1] + b[1]) / 2)
            route_keys.append((k, trips))

    if not all_x:
        return Counter()

    nearest, dists = ox.distance.nearest_edges(graph, X=all_x, Y=all_y, return_dist=True)
    hits, seen, skipped_far = Counter(), set(), 0
    for (k, trips), edge, dist in zip(route_keys, nearest, dists):
        if dist > MAX_SNAP_DISTANCE_M:
            skipped_far += 1
            continue
        rid = (k, edge[0], edge[1], edge[2])
        if rid in seen:
            continue
        seen.add(rid)
        hits[edge] += trips

    if skipped_far:
        print(f"{year}: wegen Snap-Distanz > {MAX_SNAP_DISTANCE_M}m verworfen: {skipped_far}")
    return hits


def render_year(graph, edges, route_cache, year, pairs):
    hits = edge_hits_for_year(graph, edges, route_cache, year, pairs)
    if not hits:
        print(f"{year}: keine Treffer")
        return

    total_crossings = float(np.sum(list(hits.values())))
    share_pct = 100.0 * np.array(list(hits.values()), dtype=float) / total_crossings
    vmin = max(float(np.percentile(share_pct, 5)), 1e-6)
    vmax = max(float(np.percentile(share_pct, 99.5)), vmin * 1.01)
    norm = LogNorm(vmin=vmin, vmax=vmax)
    cmap = plt.colormaps.get_cmap("turbo")

    fig, ax = ox.plot_graph(
        graph,
        node_size=0,
        edge_color="gray",
        edge_linewidth=0.4,
        bgcolor="white",
        show=False,
        close=False,
        figsize=(14, 14),
    )

    for edge, n in sorted(hits.items(), key=lambda item: item[1]):
        share = 100.0 * float(n) / total_crossings
        geom = edges.loc[edge].geometry
        lines = [geom] if geom.geom_type == "LineString" else geom.geoms
        width = 1.2 + 2.0 * np.sqrt(min(share / vmax, 1.0))
        color = cmap(norm(share))
        for line in lines:
            ax.plot(*line.xy, color=color, linewidth=width, alpha=0.9)

    hit_geoms = gpd.GeoSeries([edges.loc[edge].geometry for edge in hits], crs=edges.crs)
    minx, miny, maxx, maxy = hit_geoms.total_bounds
    pad_x = 0.05 * max(maxx - minx, 1e-6)
    pad_y = 0.05 * max(maxy - miny, 1e-6)
    ax.set_xlim(minx - pad_x, maxx + pad_x)
    ax.set_ylim(miny - pad_y, maxy + pad_y)

    cbar = fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax, fraction=0.03, pad=0.01)
    cbar.set_label("Anteil an gesamten Kanten-Uberfahrten [%] (log)")
    ax.set_title(f"Stadtradeln {year}: Kantenanteile auf erweitertem Bike-Graph")

    out = DATA_DIR / f"stadtradeln_osrm_heatmap_{year}.jpg"
    fig.savefig(out, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"{year}: gespeichert -> {out}")
    print(f"{year}: Anteilsskala (5.-99.5. Perzentil): {vmin:.6f}% bis {vmax:.6f}%")


def main():
    route_cache = json.loads(ROUTE_CACHE_FILE.read_text(encoding="utf-8"))
    pairs_by_year = {year: load_pairs(year) for year in YEARS}

    graph = load_or_build_expanded_graph(route_cache, pairs_by_year)

    if not GRAPH_FILE.exists():
        mannheim_graph = ox.graph_from_place("Mannheim, Germany", network_type="bike", simplify=True)
        ox.save_graphml(mannheim_graph, GRAPH_FILE)

    edges = ox.graph_to_gdfs(graph, nodes=False, edges=True).copy()
    for year in YEARS:
        render_year(graph, edges, route_cache, year, pairs_by_year[year])


if __name__ == "__main__":
    main()
