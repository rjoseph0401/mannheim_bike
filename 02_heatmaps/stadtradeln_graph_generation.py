import time
from pathlib import Path

import geopandas as gpd
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import osmnx as ox
from matplotlib import colors
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.lines import Line2D
from shapely.geometry import Point, box

matplotlib.rcParams["savefig.facecolor"] = "white"

# ============================================================
# Basisverzeichnisse
# ============================================================
try:
    BASE_DIR = Path(__file__).resolve().parent.parent
except NameError:
    BASE_DIR = Path.cwd()

DATA_DIR = BASE_DIR / "Data"
OUT_DIR = DATA_DIR / "outputs"
RESULTS_DIR = BASE_DIR / "Results"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# Laufkonfiguration
# ============================================================
# Zwischen 2022 / 2023 / 2024 umschalten
# RUN_TAG = "stadtradeln_2023"
RUN_TAG = "stadtradeln_2022"
# RUN_TAG = "stadtradeln_2024"

YEAR_TAG = RUN_TAG.split("_")[-1]

GPKG_CANDIDATES = [
    OUT_DIR / f"stadtradeln_graphhopper_routes_{YEAR_TAG}.gpkg",
    DATA_DIR / f"stadtradeln_graphhopper_routes_{YEAR_TAG}.gpkg",
    OUT_DIR / f"{RUN_TAG}_graphhopper_routes.gpkg",
    DATA_DIR / f"{RUN_TAG}_graphhopper_routes.gpkg",
    OUT_DIR / "stadtradeln_graphhopper_routes.gpkg",   # Altformat
    DATA_DIR / "stadtradeln_graphhopper_routes.gpkg",  # Altformat
]

for p in GPKG_CANDIDATES:
    if p.exists():
        GPKG_FILE = p
        break
else:
    raise FileNotFoundError(
        "GeoPackage nicht gefunden. Erwartet an:\n"
        + "\n".join(str(p) for p in GPKG_CANDIDATES)
    )

GPKG_LAYER = "routes"
GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"

# Dateiname wird nach SCALE_MODE und APPLY_CALIBRATION gesetzt (nach dem Plot-Abschnitt)

# ============================================================
# Parameter
# ============================================================

SCALE_MODE = "city_classes"   # "city_classes", "percentile", "share"

# Segment-Sampling:
# 1 = alle Segmente, 2 = jeder zweite, 3 = jeder dritte ...
SEGMENT_SAMPLE_STEP = 2

# Soft Matching:
SOFT_MAX_DIST = 100.0  # Punkte weiter als 100m von jeder Graphkante werden ignoriert
SOFT_K = 4
SOFT_BANDWIDTH = 25.0

# Fallback deaktiviert: Routen außerhalb des Graphs sollen nicht auf Randkanten gelegt werden
FALLBACK_TO_NEAREST_EDGE = False

LINEWIDTH_MIN = 1.0
LINEWIDTH_MAX = 3.2

VMIN_PERCENTILE = 5
VMAX_PERCENTILE = 99.5

# Kalibrierung der Hits für die Darstellung
APPLY_CALIBRATION = True
CALIB_ALPHA = 0.85
CALIB_SCALE = 1.0

CITY_BREAKS = [0, 10, 50, 150, 300, 500, np.inf]
CITY_LABELS = ["0 - 10", "10 - 50", "50 - 150", "150 - 300", "300 - 500", "ab 500"]

CITY_COLORS = [
    "#abf978",
    "#98af3b",
    "#eafe31",
    "#fcba78",
    "#fb0524",
    "#933756",
]

CUSTOM_CMAP = LinearSegmentedColormap.from_list("mannheim_custom", CITY_COLORS, N=256)

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


def classify_city_scale(value, breaks=CITY_BREAKS, labels=CITY_LABELS, colors_list=CITY_COLORS):
    for i in range(len(breaks) - 1):
        if breaks[i] <= value < breaks[i + 1]:
            return labels[i], colors_list[i], i
    return labels[-1], colors_list[-1], len(labels) - 1


def gaussian_like_weight(dist, bandwidth):
    return np.exp(-dist / bandwidth)


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


def calibrate_hits(hits_dict, alpha=0.72, scale=1.0):
    calibrated = {}
    for k, v in hits_dict.items():
        if v <= 0:
            calibrated[k] = 0.0
        else:
            calibrated[k] = scale * (v ** alpha)
    return calibrated


# ============================================================
# [1/7] Daten laden
# ============================================================

t0 = time.time()
print("[1/7] Lade Routen ...")
print(f"  RUN_TAG: {RUN_TAG}")
print(f"  YEAR_TAG: {YEAR_TAG}")
print(f"  GPKG: {GPKG_FILE}")

gdf_routes = gpd.read_file(GPKG_FILE, layer=GPKG_LAYER)
gdf_routes = gdf_routes.dropna(subset=["geometry"]).copy()
gdf_routes = gdf_routes[gdf_routes.geometry.geom_type.isin(["LineString", "MultiLineString"])].copy()

if "trips" not in gdf_routes.columns:
    raise ValueError("Spalte 'trips' fehlt im GeoPackage-Layer 'routes'.")

gdf_routes["trips"] = gdf_routes["trips"].fillna(0).astype(float)
gdf_routes = gdf_routes[gdf_routes["trips"] > 0].copy()

needed_cols = ["slon", "slat", "elon", "elat"]
if not all(c in gdf_routes.columns for c in needed_cols):
    print("  OD-Spalten fehlen, leite Start/Ende aus Geometrie ab ...")
    gdf_routes = derive_od_columns_from_geometry(gdf_routes)

print(f"  Datei: {GPKG_FILE}")
print(f"  Valide Routen: {len(gdf_routes):,}")
print(f"  Trips-Range: min={gdf_routes['trips'].min():.0f}, max={gdf_routes['trips'].max():.0f}")

# ============================================================
# [2/7] Graph laden + projizieren
# ============================================================

print("[2/7] Lade / projiziere Fahrradgraph ...")

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
graph_crs = edges_proj.crs

gdf_routes = gdf_routes.to_crs(graph_crs)

edge_geom = edges_proj.geometry.to_dict()
edge_keys = list(edges_proj.index)
sindex = edges_proj.sindex

print(f"  CRS Graph: {graph_crs}")
print(f"  Kanten im Graph: {len(edges_proj):,}")
print(f"  Dauer: {fmt_seconds(time.time() - t_graph)}")

# ============================================================
# [3/7] Routen in Segmente zerlegen
# ============================================================

print("[3/7] Zerlege Routen in Segmente ...")

t_segments = time.time()

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

        if SEGMENT_SAMPLE_STEP > 1:
            idx = np.arange(0, len(lengths), SEGMENT_SAMPLE_STEP)
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

print(f"  Verwendete Segment-Mittelpunkte: {len(seg_route_ids):,}")
print(f"  Sampling-Schritt: {SEGMENT_SAMPLE_STEP}")
print(f"  Dauer: {fmt_seconds(time.time() - t_segments)}")

if len(seg_route_ids) == 0:
    raise ValueError("Keine Segmente erzeugt. Prüfe die Routen-Geometrien.")

# ============================================================
# [4/7] Soft Matching
# ============================================================

print("[4/7] Soft-Matching auf mehrere nahe Kanten ...")

t_match = time.time()

route_edge_score = {}
n_points_without_candidate = 0

for idx, (rid, x, y, seg_len) in enumerate(zip(seg_route_ids, all_x, all_y, seg_lengths), 1):
    query_geom = box(x - SOFT_MAX_DIST, y - SOFT_MAX_DIST, x + SOFT_MAX_DIST, y + SOFT_MAX_DIST)
    cand_idx = list(sindex.query(query_geom, predicate="intersects"))

    if not cand_idx:
        n_points_without_candidate += 1
        continue

    dists = []
    px = Point(x, y)

    for j in cand_idx:
        geom = edge_geom[edge_keys[j]]
        d = geom.distance(px)
        if d <= SOFT_MAX_DIST:
            dists.append((j, d))

    if not dists:
        n_points_without_candidate += 1
        continue

    dists.sort(key=lambda t: t[1])
    dists = dists[:SOFT_K]

    ds = np.array([d for _, d in dists], dtype=float)
    ws = gaussian_like_weight(ds, SOFT_BANDWIDTH)

    wsum = ws.sum()
    if wsum <= 0:
        n_points_without_candidate += 1
        continue

    ws = ws / wsum

    for (j, _d), w in zip(dists, ws):
        edge_key = edge_keys[j]
        route_edge_score[(rid, edge_key)] = route_edge_score.get((rid, edge_key), 0.0) + seg_len * w

    if idx % 100000 == 0:
        print(f"  verarbeitet: {idx:,}/{len(seg_route_ids):,}")

print(f"  Segmentpunkte ohne Kandidaten: {n_points_without_candidate:,}")
print(f"  Route-Kante-Zuordnungen: {len(route_edge_score):,}")
print(f"  Dauer: {fmt_seconds(time.time() - t_match)}")

# ============================================================
# [5/7] Massenerhaltende Aggregation
# ============================================================

print("[5/7] Aggregiere Kantenhits massenerhaltend ...")

t_hits = time.time()

route_total_score = {}
for (rid, edge_key), score in route_edge_score.items():
    route_total_score[rid] = route_total_score.get(rid, 0.0) + score

n_fallback_routes = 0
hits = {}

if FALLBACK_TO_NEAREST_EDGE:
    route_nearest_edges = ox.distance.nearest_edges(G_proj, X=route_mid_x, Y=route_mid_y)
else:
    route_nearest_edges = None

for (rid, edge_key), score in route_edge_score.items():
    total_score = route_total_score.get(rid, 0.0)
    trips = trip_values[rid]

    if total_score <= 0 or trips <= 0:
        continue

    contribution = trips * (score / total_score)
    hits[edge_key] = hits.get(edge_key, 0.0) + contribution

if FALLBACK_TO_NEAREST_EDGE:
    for rid in range(len(gdf_routes)):
        if trip_values[rid] <= 0:
            continue
        if route_total_score.get(rid, 0.0) > 0:
            continue

        e = route_nearest_edges[rid]
        edge_key = (e[0], e[1], e[2])
        hits[edge_key] = hits.get(edge_key, 0.0) + trip_values[rid]
        n_fallback_routes += 1

print(f"  Fallback-Routen ohne Soft-Match: {n_fallback_routes:,}")
print(f"  Gematchte Kanten: {len(hits):,}")
print(f"  Dauer: {fmt_seconds(time.time() - t_hits)}")

if not hits:
    raise ValueError("Keine Kantenhits erzeugt.")

# ============================================================
# [6/7] Kontrollsummen + Kalibrierung
# ============================================================

print("[6/7] Kontrollsummen ...")

total_hits = float(sum(hits.values()))
total_trips = float(gdf_routes["trips"].sum())

print("Total model flow:", total_hits)
print("Total input trips:", total_trips)
print("Flow / Trips ratio:", total_hits / total_trips if total_trips > 0 else np.nan)

if APPLY_CALIBRATION:
    hits_plot = calibrate_hits(
        hits,
        alpha=CALIB_ALPHA,
        scale=CALIB_SCALE,
    )
    print("Kalibrierung aktiv:")
    print("  alpha =", CALIB_ALPHA)
    print("  scale =", CALIB_SCALE)
else:
    hits_plot = hits
    print("Kalibrierung deaktiviert.")

# ============================================================
# [7/7] Plot
# ============================================================

print("[7/7] Zeichne Karte ...")

t_plot = time.time()

fig, ax = ox.plot_graph(
    G_proj,
    node_size=0,
    node_color="none",
    edge_color="#d7d7d7",
    edge_linewidth=0.35,
    bgcolor="white",
    show=False,
    close=False,
    figsize=(14, 14),
)

legend_handles = None
cbar_label = None
norm = None

if SCALE_MODE == "percentile":
    plot_hits = {k: float(v) for k, v in hits_plot.items()}
    vals = np.array(list(plot_hits.values()), dtype=float)

    vmin = max(1e-9, np.percentile(vals, VMIN_PERCENTILE))
    vmax = max(vmin * 1.0001, np.percentile(vals, VMAX_PERCENTILE))

    norm = colors.LogNorm(vmin=vmin, vmax=vmax)
    cbar_label = f"Kalibrierte Trips (P{VMIN_PERCENTILE}–P{VMAX_PERCENTILE}, log)"

elif SCALE_MODE == "share":
    total = float(sum(hits_plot.values()))
    plot_hits = {k: v / total for k, v in hits_plot.items()}
    vals = np.array(list(plot_hits.values()), dtype=float)
    vals = vals[vals > 0]

    vmin = vals.min()
    vmax = vals.max()

    norm = colors.LogNorm(vmin=vmin, vmax=vmax)
    cbar_label = "Anteil an Gesamtgewicht (Kante / Summe aller count)"

elif SCALE_MODE == "city_classes":
    plot_hits = {k: float(v) for k, v in hits_plot.items()}
    legend_handles = [
        Line2D([0], [0], color=color, lw=4, label=label)
        for color, label in zip(CITY_COLORS, CITY_LABELS)
    ]

else:
    raise ValueError("Ungültiger SCALE_MODE")

vmax_width = max(hits_plot.values())

for (u, v, k), val in plot_hits.items():
    geom = edge_geom[(u, v, k)]
    lines = [geom] if geom.geom_type == "LineString" else list(geom.geoms)

    n_abs = hits_plot[(u, v, k)]
    width = LINEWIDTH_MIN + (LINEWIDTH_MAX - LINEWIDTH_MIN) * (n_abs / vmax_width)

    if SCALE_MODE == "city_classes":
        _, line_color, _ = classify_city_scale(n_abs)
    else:
        line_color = plt.cm.turbo(norm(val))

    for line in lines:
        ax.plot(
            *line.xy,
            color=line_color,
            linewidth=width,
            alpha=0.92,
            solid_capstyle="round",
        )

if SCALE_MODE in ("percentile", "share"):
    cbar = fig.colorbar(
        plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.turbo),
        ax=ax,
        fraction=0.03,
        pad=0.01,
    )
    cbar.set_label(cbar_label)

elif SCALE_MODE == "city_classes":
    ax.legend(
        handles=legend_handles,
        title="Anzahl Radfahrender\npro Streckensegment",
        loc="lower right",
        frameon=True,
        facecolor="white",
        edgecolor="black",
    )

YEAR_SHORT = YEAR_TAG[-2:]  # "2022" -> "22"

if SCALE_MODE == "percentile":
    OUTPUT_FILE = RESULTS_DIR / f"graphhopper_stadtradeln{YEAR_SHORT}_heatmap_percentile.png"
    ax.set_title(f"Stadtradeln {RUN_TAG} (Soft-Spread, kalibriert, Perzentil-Skalierung)")
elif SCALE_MODE == "share":
    OUTPUT_FILE = RESULTS_DIR / f"graphhopper_stadtradeln{YEAR_SHORT}_heatmap_share.png"
    ax.set_title(f"Stadtradeln {RUN_TAG} (Soft-Spread, kalibriert, Anteil am Gesamtfluss)")
else:  # city_classes
    is_identity = (CALIB_ALPHA == 1.0 and CALIB_SCALE == 1.0)
    suffix = "unskaliert" if (not APPLY_CALIBRATION or is_identity) else "skaliert"
    OUTPUT_FILE = RESULTS_DIR / f"graphhopper_stadtradeln{YEAR_SHORT}_heatmap_stadtvergleich_{suffix}.png"
    ax.set_title(f"Stadtradeln {RUN_TAG} (Soft-Spread, kalibriert, Klassenskala wie Stadt Mannheim)")

fig.savefig(OUTPUT_FILE, dpi=300, bbox_inches="tight", facecolor="white")
print("  Gespeichert:", OUTPUT_FILE)
print(f"  Plot-Dauer: {fmt_seconds(time.time() - t_plot)}")
print(f"  Gesamt-Dauer: {fmt_seconds(time.time() - t0)}")

plt.show()