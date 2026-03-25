import osmnx as ox
import matplotlib.pyplot as plt
from pathlib import Path
from matplotlib import cm, colors
import geopandas as gpd
import numpy as np
from shapely.geometry import Point
from pathlib import Path

# ============================================================
# Basisverzeichnisse
# ============================================================
try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path.cwd()

DATA_DIR = BASE_DIR / "Data"
OUT_DIR = DATA_DIR / "outputs"

# ============================================================
# Dateien
# ============================================================

GPKG_FILE = OUT_DIR / "stadtradeln_graphhopper_routes.gpkg"
GPKG_LAYER = "routes"

GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"

OUTPUT_FILE = OUT_DIR / "mannheim_stadtradeln_heatmap_weighted.png"

# ============================================================
# Parameter
# ============================================================

SCALE_MODE = "share"        # "percentile" oder "share"

MAX_MATCH_DIST = 40
LINEWIDTH_MIN = 1.0
LINEWIDTH_MAX = 3.0

VMIN_PERCENTILE = 5
VMAX_PERCENTILE = 99.5

# ============================================================
# Daten laden
# ============================================================

gdf_routes = gpd.read_file(GPKG_FILE, layer=GPKG_LAYER)
gdf_routes = gdf_routes.dropna(subset=["geometry"]).copy()
gdf_routes = gdf_routes[gdf_routes.geometry.geom_type.isin(["LineString", "MultiLineString"])].copy()

if "trips" not in gdf_routes.columns:
    raise ValueError("Spalte 'trips' fehlt.")

gdf_routes["trips"] = gdf_routes["trips"].fillna(1).astype(int)

print(f"Valide Routen: {len(gdf_routes)}")
print(f"Trips-Range: min={gdf_routes['trips'].min()}, max={gdf_routes['trips'].max()}")

# ============================================================
# Graph laden + projizieren
# ============================================================

if GRAPH_FILE.exists():
    G = ox.load_graphml(GRAPH_FILE)
else:
    G = ox.graph_from_place("Mannheim, Germany", network_type="bike", simplify=True)
    ox.save_graphml(G, GRAPH_FILE)

G_proj = ox.project_graph(G)
edges_proj = ox.graph_to_gdfs(G_proj, nodes=False, edges=True).copy()
graph_crs = edges_proj.crs

gdf_routes = gdf_routes.to_crs(graph_crs)

# ============================================================
# Segment-Mittelpunkte
# ============================================================

all_x, all_y, route_ids = [], [], []

for rid, row in enumerate(gdf_routes.itertuples()):
    geom = row.geometry
    parts = [geom] if geom.geom_type == "LineString" else list(geom.geoms)

    for part in parts:
        coords = list(part.coords)
        for i in range(len(coords) - 1):
            x1, y1 = coords[i]
            x2, y2 = coords[i + 1]
            all_x.append((x1 + x2) / 2)
            all_y.append((y1 + y2) / 2)
            route_ids.append(rid)

print(f"Segmente: {len(route_ids):,}")

# ============================================================
# Nearest edges
# ============================================================

nearest = ox.distance.nearest_edges(G_proj, X=all_x, Y=all_y)

# ============================================================
# Hits berechnen
# ============================================================

hits = {}
seen = set()
trip_values = gdf_routes["trips"].tolist()

for rid, edge, x, y in zip(route_ids, nearest, all_x, all_y):

    edge_key = (edge[0], edge[1], edge[2])
    dedup_key = (rid, *edge_key)

    if dedup_key in seen:
        continue

    geom = edges_proj.loc[edge_key].geometry
    if geom.distance(Point(x, y)) > MAX_MATCH_DIST:
        continue

    seen.add(dedup_key)
    hits[edge_key] = hits.get(edge_key, 0) + trip_values[rid]

print(f"Gematchte Kanten: {len(hits)}")

# ============================================================
# Plot
# ============================================================

fig, ax = ox.plot_graph(
    G_proj,
    node_size=0,
    node_color="none",
    edge_color="gray",
    edge_linewidth=0.4,
    bgcolor="white",
    show=False,
    close=False,
    figsize=(14, 14),
)

# ============================================================
# Skalierung
# ============================================================

if SCALE_MODE == "percentile":

    plot_hits = {k: float(v) for k, v in hits.items()}
    vals = np.array(list(plot_hits.values()))

    vmin = max(1.0, np.percentile(vals, VMIN_PERCENTILE))
    vmax = np.percentile(vals, VMAX_PERCENTILE)
    vmax = max(vmax, vmin + 1)

    norm = colors.LogNorm(vmin=vmin, vmax=vmax)
    cbar_label = "Trips (Perzentil-Skalierung)"

elif SCALE_MODE == "share":

    total = float(sum(hits.values()))
    plot_hits = {k: v / total for k, v in hits.items()}
    vals = np.array(list(plot_hits.values()))

    vals = vals[vals > 0]
    vmin = vals.min()
    vmax = vals.max()

    norm = colors.LogNorm(vmin=vmin, vmax=vmax)
    cbar_label = "Anteil an Gesamttrips"

    print(f"Gesamttrips: {total:,.0f}")

else:
    raise ValueError("Ungültiger SCALE_MODE")

# ============================================================
# Zeichnen
# ============================================================

cmap = cm.get_cmap("turbo")
vmax_width = max(hits.values())

for (u, v, k), val in plot_hits.items():

    geom = edges_proj.loc[(u, v, k)].geometry
    lines = [geom] if geom.geom_type == "LineString" else list(geom.geoms)

    n_abs = hits[(u, v, k)]
    width = LINEWIDTH_MIN + (LINEWIDTH_MAX - LINEWIDTH_MIN) * (n_abs / vmax_width)

    for line in lines:
        ax.plot(
            *line.xy,
            color=cmap(norm(val)),
            linewidth=width,
            alpha=0.9,
            solid_capstyle="round",
        )

# ============================================================
# Colorbar
# ============================================================

cbar = fig.colorbar(
    cm.ScalarMappable(norm=norm, cmap=cmap),
    ax=ax,
    fraction=0.03,
    pad=0.01,
)
cbar.set_label(cbar_label)

# ============================================================
# Titel + Save
# ============================================================

if SCALE_MODE == "percentile":
    ax.set_title("Stadtradeln (Perzentil-Skalierung)")
else:
    ax.set_title("Stadtradeln (Anteil an Gesamttrips)")

fig.savefig(OUTPUT_FILE, dpi=300, bbox_inches="tight")
print("Gespeichert:", OUTPUT_FILE)

plt.show()