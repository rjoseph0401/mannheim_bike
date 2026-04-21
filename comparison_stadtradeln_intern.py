import time
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import osmnx as ox
from matplotlib import colors
from shapely.geometry import Point, box

# ============================================================
# Basisverzeichnisse
# ============================================================
try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path.cwd()

DATA_DIR = BASE_DIR / "Data"
OUT_DIR = DATA_DIR / "outputs"
RESULTS_DIR = BASE_DIR / "Results"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# Konfiguration
# ============================================================

YEAR_A = "2022"   # wird abgezogen
YEAR_B = "2023"   # minus YEAR_A

RUN_TAG = f"delta_{YEAR_B}_minus_{YEAR_A}"

GPKG_A = OUT_DIR / f"stadtradeln_graphhopper_routes_{YEAR_A}.gpkg"
GPKG_B = OUT_DIR / f"stadtradeln_graphhopper_routes_{YEAR_B}.gpkg"

GPKG_LAYER = "routes"
GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"
OUTPUT_FILE = RESULTS_DIR / f"{RUN_TAG}.png"

# Soft Matching wie im bisherigen Plot-Skript
SEGMENT_SAMPLE_STEP = 2
SOFT_MAX_DIST = 60.0
SOFT_K = 4
SOFT_BANDWIDTH = 25.0
FALLBACK_TO_NEAREST_EDGE = True

# Delta-Plot
LINEWIDTH_MIN = 0.6
LINEWIDTH_MAX = 3.8
DELTA_PERCENTILE = 99.0   # für Farbskalen-Cutoff

# optionale Kalibrierung
APPLY_CALIBRATION = True
CALIB_ALPHA = 1.0
CALIB_SCALE = 1.0


# ============================================================
# Hilfsfunktionen
# ============================================================

def fmt_seconds(seconds: float) -> str:
    seconds = int(round(seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def gaussian_like_weight(dist, bandwidth):
    return np.exp(-dist / bandwidth)


def calibrate_hits(hits_dict, alpha=0.85, scale=1.0):
    calibrated = {}
    for k, v in hits_dict.items():
        if v <= 0:
            calibrated[k] = 0.0
        else:
            calibrated[k] = scale * (v ** alpha)
    return calibrated


def derive_od_columns_from_geometry(gdf):
    slons, slats, elons, elats = [], [], [], []

    for geom in gdf.geometry:
        if geom is None or geom.is_empty:
            slons.append(np.nan)
            slats.append(np.nan)
            elons.append(np.nan)
            elats.append(np.nan)
            continue

        if geom.geom_type == "LineString":
            coords = list(geom.coords)
            slon, slat = coords[0]
            elon, elat = coords[-1]
        elif geom.geom_type == "MultiLineString":
            parts = list(geom.geoms)
            if not parts:
                slons.append(np.nan)
                slats.append(np.nan)
                elons.append(np.nan)
                elats.append(np.nan)
                continue
            first_coords = list(parts[0].coords)
            last_coords = list(parts[-1].coords)
            slon, slat = first_coords[0]
            elon, elat = last_coords[-1]
        else:
            slons.append(np.nan)
            slats.append(np.nan)
            elons.append(np.nan)
            elats.append(np.nan)
            continue

        slons.append(slon)
        slats.append(slat)
        elons.append(elon)
        elats.append(elat)

    gdf["slon"] = slons
    gdf["slat"] = slats
    gdf["elon"] = elons
    gdf["elat"] = elats
    return gdf


def load_routes(gpkg_file, layer="routes"):
    gdf = gpd.read_file(gpkg_file, layer=layer)
    gdf = gdf.dropna(subset=["geometry"]).copy()
    gdf = gdf[gdf.geometry.geom_type.isin(["LineString", "MultiLineString"])].copy()

    if "trips" not in gdf.columns:
        raise ValueError(f"Spalte 'trips' fehlt in {gpkg_file.name}, Layer '{layer}'.")

    gdf["trips"] = gdf["trips"].fillna(0).astype(float)
    gdf = gdf[gdf["trips"] > 0].copy()

    needed_cols = ["slon", "slat", "elon", "elat"]
    if not all(c in gdf.columns for c in needed_cols):
        gdf = derive_od_columns_from_geometry(gdf)

    return gdf


def compute_edge_hits_for_routes(
    gdf_routes,
    G_proj,
    edges_proj,
    segment_sample_step=2,
    soft_max_dist=60.0,
    soft_k=4,
    soft_bandwidth=25.0,
    fallback_to_nearest_edge=True,
):
    gdf_routes = gdf_routes.to_crs(edges_proj.crs)

    edge_geom = edges_proj.geometry.to_dict()
    edge_keys = list(edges_proj.index)
    sindex = edges_proj.sindex

    all_x = []
    all_y = []
    seg_route_ids = []
    seg_lengths = []
    route_mid_x = []
    route_mid_y = []

    trip_values = gdf_routes["trips"].to_numpy()

    for rid, row in enumerate(gdf_routes.itertuples()):
        geom = row.geometry
        parts = [geom] if geom.geom_type == "LineString" else list(geom.geoms)

        route_mid = geom.interpolate(0.5, normalized=True)
        route_mid_x.append(route_mid.x)
        route_mid_y.append(route_mid.y)

        for part in parts:
            coords = np.asarray(part.coords, dtype=float)
            if len(coords) < 2:
                continue

            diffs = coords[1:] - coords[:-1]
            lengths = np.sqrt((diffs ** 2).sum(axis=1))

            if segment_sample_step > 1:
                idx = np.arange(0, len(lengths), segment_sample_step)
                mids = (coords[:-1][idx] + coords[1:][idx]) / 2.0
                lengths_use = lengths[idx]
            else:
                mids = (coords[:-1] + coords[1:]) / 2.0
                lengths_use = lengths

            all_x.extend(mids[:, 0].tolist())
            all_y.extend(mids[:, 1].tolist())
            seg_route_ids.extend([rid] * len(mids))
            seg_lengths.extend(lengths_use.tolist())

    all_x = np.asarray(all_x, dtype=float)
    all_y = np.asarray(all_y, dtype=float)
    seg_route_ids = np.asarray(seg_route_ids, dtype=np.int32)
    seg_lengths = np.asarray(seg_lengths, dtype=float)
    route_mid_x = np.asarray(route_mid_x, dtype=float)
    route_mid_y = np.asarray(route_mid_y, dtype=float)

    if len(seg_route_ids) == 0:
        raise ValueError("Keine Segmente erzeugt. Prüfe die Routen-Geometrien.")

    route_edge_score = {}
    route_total_score = {}
    n_points_without_candidate = 0

    for rid, x, y, seg_len in zip(seg_route_ids, all_x, all_y, seg_lengths):
        query_geom = box(x - soft_max_dist, y - soft_max_dist, x + soft_max_dist, y + soft_max_dist)
        cand_idx = list(sindex.query(query_geom, predicate="intersects"))

        if not cand_idx:
            n_points_without_candidate += 1
            continue

        dists = []
        px = Point(x, y)

        for j in cand_idx:
            geom = edge_geom[edge_keys[j]]
            d = geom.distance(px)
            if d <= soft_max_dist:
                dists.append((j, d))

        if not dists:
            n_points_without_candidate += 1
            continue

        dists.sort(key=lambda t: t[1])
        dists = dists[:soft_k]

        ds = np.array([d for _, d in dists], dtype=float)
        ws = gaussian_like_weight(ds, soft_bandwidth)

        wsum = ws.sum()
        if wsum <= 0:
            n_points_without_candidate += 1
            continue

        ws = ws / wsum

        for (j, _d), w in zip(dists, ws):
            edge_key = edge_keys[j]
            score = seg_len * w
            route_edge_score[(rid, edge_key)] = route_edge_score.get((rid, edge_key), 0.0) + score
            route_total_score[rid] = route_total_score.get(rid, 0.0) + score

    hits = {}

    for (rid, edge_key), score in route_edge_score.items():
        total_score = route_total_score.get(rid, 0.0)
        trips = trip_values[rid]

        if total_score <= 0 or trips <= 0:
            continue

        contribution = trips * (score / total_score)
        hits[edge_key] = hits.get(edge_key, 0.0) + contribution

    n_fallback_routes = 0

    if fallback_to_nearest_edge:
        route_nearest_edges = ox.distance.nearest_edges(G_proj, X=route_mid_x, Y=route_mid_y)

        for rid in range(len(gdf_routes)):
            if trip_values[rid] <= 0:
                continue
            if route_total_score.get(rid, 0.0) > 0:
                continue

            e = route_nearest_edges[rid]
            edge_key = (e[0], e[1], e[2])
            hits[edge_key] = hits.get(edge_key, 0.0) + trip_values[rid]
            n_fallback_routes += 1

    return hits, {
        "n_routes": len(gdf_routes),
        "n_segments": len(seg_route_ids),
        "n_points_without_candidate": n_points_without_candidate,
        "n_matched_edges": len(hits),
        "n_fallback_routes": n_fallback_routes,
        "total_trips": float(trip_values.sum()),
    }


# ============================================================
# [1/6] Daten laden
# ============================================================

t0 = time.time()
print("[1/6] Lade Routen ...")
print(f"  A (wird abgezogen): {YEAR_A}")
print(f"  B (minus A):        {YEAR_B}")
print(f"  GPKG A: {GPKG_A}")
print(f"  GPKG B: {GPKG_B}")

gdf_A = load_routes(GPKG_A, GPKG_LAYER)
gdf_B = load_routes(GPKG_B, GPKG_LAYER)

print(f"  {YEAR_A}: {len(gdf_A):,} valide Routen")
print(f"  {YEAR_B}: {len(gdf_B):,} valide Routen")

# ============================================================
# [2/6] Graph laden
# ============================================================

print("[2/6] Lade / projiziere Fahrradgraph ...")
t_graph = time.time()

if GRAPH_FILE.exists():
    G = ox.load_graphml(GRAPH_FILE)
    print(f"  Graph geladen: {GRAPH_FILE.name}")
else:
    G = ox.graph_from_place("Mannheim, Germany", network_type="bike", simplify=True)
    ox.save_graphml(G, GRAPH_FILE)
    print(f"  Graph neu erzeugt und gespeichert: {GRAPH_FILE.name}")

G_proj = ox.project_graph(G)
edges_proj = ox.graph_to_gdfs(G_proj, nodes=False, edges=True).copy()
edge_geom = edges_proj.geometry.to_dict()

print(f"  CRS Graph: {edges_proj.crs}")
print(f"  Kanten im Graph: {len(edges_proj):,}")
print(f"  Dauer: {fmt_seconds(time.time() - t_graph)}")

# ============================================================
# [3/6] Kantenhits je Jahr berechnen
# ============================================================

print("[3/6] Berechne Kantenhits für beide Jahre ...")

t_hits = time.time()

hits_A, stats_A = compute_edge_hits_for_routes(
    gdf_A,
    G_proj,
    edges_proj,
    segment_sample_step=SEGMENT_SAMPLE_STEP,
    soft_max_dist=SOFT_MAX_DIST,
    soft_k=SOFT_K,
    soft_bandwidth=SOFT_BANDWIDTH,
    fallback_to_nearest_edge=FALLBACK_TO_NEAREST_EDGE,
)

hits_B, stats_B = compute_edge_hits_for_routes(
    gdf_B,
    G_proj,
    edges_proj,
    segment_sample_step=SEGMENT_SAMPLE_STEP,
    soft_max_dist=SOFT_MAX_DIST,
    soft_k=SOFT_K,
    soft_bandwidth=SOFT_BANDWIDTH,
    fallback_to_nearest_edge=FALLBACK_TO_NEAREST_EDGE,
)

print(f"  {YEAR_A}: gematchte Kanten = {stats_A['n_matched_edges']:,}, "
      f"Fallback-Routen = {stats_A['n_fallback_routes']:,}, "
      f"Trips gesamt = {stats_A['total_trips']:.0f}")

print(f"  {YEAR_B}: gematchte Kanten = {stats_B['n_matched_edges']:,}, "
      f"Fallback-Routen = {stats_B['n_fallback_routes']:,}, "
      f"Trips gesamt = {stats_B['total_trips']:.0f}")

print(f"  Dauer: {fmt_seconds(time.time() - t_hits)}")

if APPLY_CALIBRATION:
    hits_A = calibrate_hits(hits_A, alpha=CALIB_ALPHA, scale=CALIB_SCALE)
    hits_B = calibrate_hits(hits_B, alpha=CALIB_ALPHA, scale=CALIB_SCALE)
    print(f"  Kalibrierung aktiv: alpha={CALIB_ALPHA}, scale={CALIB_SCALE}")
else:
    print("  Kalibrierung deaktiviert.")

# ============================================================
# [4/6] Delta berechnen
# ============================================================

print("[4/6] Berechne Delta ...")

all_edges = set(hits_A.keys()) | set(hits_B.keys())

delta = {}
for e in all_edges:
    a = hits_A.get(e, 0.0)
    b = hits_B.get(e, 0.0)
    d = b - a
    if d != 0:
        delta[e] = d

if not delta:
    raise ValueError(
        "Delta ist leer. Wahrscheinlich konnten für keines der Jahre Kantenhits berechnet werden."
    )

vals = np.array(list(delta.values()), dtype=float)
abs_vals = np.abs(vals)
abs_vals = abs_vals[np.isfinite(abs_vals)]

if abs_vals.size == 0:
    raise ValueError("Delta enthält keine endlichen Werte.")

vmax = float(np.percentile(abs_vals, DELTA_PERCENTILE))
if not np.isfinite(vmax) or vmax <= 0:
    vmax = float(abs_vals.max())

if vmax <= 0:
    raise ValueError("Alle Delta-Werte sind 0; es gibt nichts zu visualisieren.")

vmin = -vmax
norm = colors.TwoSlopeNorm(vmin=vmin, vcenter=0.0, vmax=vmax)

print(f"  Delta-Kanten: {len(delta):,}")
print(f"  Delta-Min: {vals.min():.3f}")
print(f"  Delta-Max: {vals.max():.3f}")
print(f"  Farbskala: ±{vmax:.3f} (Perzentil {DELTA_PERCENTILE})")

# ============================================================
# [5/6] Plot
# ============================================================

print("[5/6] Zeichne Delta-Karte ...")
t_plot = time.time()

fig, ax = ox.plot_graph(
    G_proj,
    node_size=0,
    node_color="none",
    edge_color="#d9d9d9",
    edge_linewidth=0.35,
    bgcolor="white",
    show=False,
    close=False,
    figsize=(14, 14),
)

cmap = plt.get_cmap("RdBu_r")
vmax_width = max(abs_vals)

for (u, v, k), d in delta.items():
    geom = edge_geom.get((u, v, k))
    if geom is None:
        continue

    lines = [geom] if geom.geom_type == "LineString" else list(geom.geoms)

    width = LINEWIDTH_MIN + (LINEWIDTH_MAX - LINEWIDTH_MIN) * (abs(d) / vmax_width)
    line_color = cmap(norm(d))

    for line in lines:
        ax.plot(
            *line.xy,
            color=line_color,
            linewidth=width,
            alpha=0.92,
            solid_capstyle="round",
        )

cbar = fig.colorbar(
    plt.cm.ScalarMappable(norm=norm, cmap=cmap),
    ax=ax,
    fraction=0.03,
    pad=0.01,
)
cbar.set_label(f"Delta Flow ({YEAR_B} - {YEAR_A})")

ax.set_title(f"Delta-Map Stadtradeln: {YEAR_B} - {YEAR_A}")

fig.savefig(OUTPUT_FILE, dpi=300, bbox_inches="tight", facecolor="white")
print(f"  Gespeichert: {OUTPUT_FILE}")
print(f"  Plot-Dauer: {fmt_seconds(time.time() - t_plot)}")

# ============================================================
# [6/6] Abschluss
# ============================================================

print("[6/6] Fertig.")
print(f"  Gesamt-Dauer: {fmt_seconds(time.time() - t0)}")

plt.show()