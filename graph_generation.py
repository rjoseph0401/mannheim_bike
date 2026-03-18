import osmnx as ox
import matplotlib.pyplot as plt
import pandas as pd
import ast
from pathlib import Path
from matplotlib import cm, colors
import geopandas as gpd
import numpy as np

# -----------------------
# Dateien
# -----------------------
CSV_FILE = "routes_graphhopper.csv"
GEOJSON_FILE = "outputs/all_routes_graphhopper_local.geojson"

# -----------------------
# CSV laden + Route parse
# -----------------------
df_routes = pd.read_csv(CSV_FILE)
df_routes["route_als_liste"] = df_routes["GraphhopperKoordinaten"].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) and x.strip().startswith("[") else pd.NA
)

routes = df_routes["route_als_liste"].dropna()
routes = routes[routes.apply(lambda r: isinstance(r, (list, tuple)) and len(r) >= 2)]
print(f"Valide Routen: {len(routes)}")

# -----------------------
# GeoJSON laden + count matchen über komplette Route (Punktfolge)
# -----------------------
gdf_counts = gpd.read_file(GEOJSON_FILE)
gdf_counts = gdf_counts.dropna(subset=["geometry"]).copy()

def route_key_from_csv_list(route, nd=6):
    # route: [(lon,lat), ...]
    return tuple((round(lon, nd), round(lat, nd)) for lon, lat in route)

def route_key_from_linestring(geom, nd=6):
    # geom: shapely LineString
    return tuple((round(x, nd), round(y, nd)) for x, y in geom.coords)

df_routes["route_key"] = df_routes["route_als_liste"].apply(
    lambda r: route_key_from_csv_list(r) if isinstance(r, (list, tuple)) and len(r) >= 2 else pd.NA
)
gdf_counts["route_key"] = gdf_counts["geometry"].apply(lambda g: route_key_from_linestring(g))

# Falls count schon in der CSV vorhanden ist, verwende diesen direkt.
# Falls nicht, versuche count über die GeoJSON zu mappen.
if "count" in df_routes.columns:
    df_routes["count"] = pd.to_numeric(df_routes["count"], errors="coerce").fillna(1).astype(int)
    missing = int(df_routes.loc[routes.index, "count"].isna().sum()) if len(routes) > 0 else 0
    print(f"Counts aus CSV verwendet.")
else:
    counts_by_key = gdf_counts.groupby("route_key")["count"].max()
    df_routes["count"] = df_routes["route_key"].map(counts_by_key)

    missing = int(df_routes.loc[routes.index, "count"].isna().sum())
    print(f"Counts nicht gematcht (werden als 1 gesetzt): {missing}")

    df_routes["count"] = df_routes["count"].fillna(1).astype(int)

counts_for_routes = df_routes.loc[routes.index, "count"].tolist()

# -----------------------
# Graph laden/erzeugen
# -----------------------
graph_file = Path("mannheim_bike.graphml")
if graph_file.exists():
    G = ox.load_graphml(graph_file)
else:
    G = ox.graph_from_place("Mannheim, Germany", network_type="bike", simplify=True)
    ox.save_graphml(G, graph_file)

edges = ox.graph_to_gdfs(G, nodes=False, edges=True).copy()

# -----------------------
# Segment-Mittelpunkte berechnen (mit Fortschritt)
# -----------------------
all_x, all_y, route_ids = [], [], []
total_routes = len(routes)

for idx, route in enumerate(routes):
    if idx % 100 == 0:
        print(f"Verarbeite Routen: {idx}/{total_routes} ({idx/total_routes:.1%})", end="\r")

    for i in range(len(route) - 1):
        lon1, lat1 = route[i]
        lon2, lat2 = route[i + 1]
        all_x.append((lon1 + lon2) / 2)
        all_y.append((lat1 + lat2) / 2)
        route_ids.append(idx)

print(f"Verarbeite Routen: {total_routes}/{total_routes} (100%)")

# -----------------------
# Nearest edges
# -----------------------
print("Starte nearest_edges Berechnung...")
nearest = ox.distance.nearest_edges(G, X=all_x, Y=all_y) if all_x else []
print("nearest_edges abgeschlossen.")

# -----------------------
# Plot Grundgraph
# -----------------------
fig, ax = ox.plot_graph(
    G,
    node_size=0,
    node_color="limegreen",
    edge_color="gray",
    edge_linewidth=0.4,
    bgcolor="white",
    show=False,
    close=False,
    figsize=(14, 14),
)

# -----------------------
# Hits zählen (GEWICHTET mit count) + Fortschritt
# -----------------------
hits = {}
seen = set()
total_edges = len(route_ids)

for i, (rid, edge) in enumerate(zip(route_ids, nearest)):
    if i % 5000 == 0 and total_edges > 0:
        print(f"Matching Edges: {i}/{total_edges} ({i/total_edges:.1%})", end="\r")

    key = (rid, edge[0], edge[1], edge[2])
    if key not in seen:
        seen.add(key)
        w = counts_for_routes[rid]
        hits[edge] = hits.get(edge, 0) + w

if total_edges > 0:
    print(f"Matching Edges: {total_edges}/{total_edges} (100%)")

print(f"Gematchte Kanten: {len(hits)}")
if hits:
    print(f"Hit-Range (gewichtet): min={min(hits.values())}, max={max(hits.values())}")

# -----------------------
# Heatmap zeichnen
# -----------------------
if hits:
    vmax = max(hits.values())

    vals = np.array(list(hits.values()))
    vmin = max(1, int(np.percentile(vals, 5)))
    vmax = int(np.percentile(vals, 99.5))
    vmax = max(vmax, vmin + 1)

    norm = colors.LogNorm(vmin=vmin, vmax=vmax)
    cmap = cm.get_cmap("turbo")

    for (u, v, k), n in hits.items():
        geom = edges.loc[(u, v, k)].geometry
        line_list = [geom] if geom.geom_type == "LineString" else geom.geoms
        for line in line_list:
            ax.plot(
                *line.xy,
                color=cmap(norm(n)),
                linewidth=1.5 + 2.5 * n / vmax,
                alpha=0.9,
            )

    cbar = fig.colorbar(
        cm.ScalarMappable(norm=norm, cmap=cmap),
        ax=ax,
        fraction=0.03,
        pad=0.01,
    )
    cbar.set_label("Gewichtete Häufigkeit (Summe count)")

ax.set_title("Graphhopper-Routen (gewichtet nach Häufigkeit) als Heatmap auf Mannheim Graph")

output_file = "mannheim_graphhopper_heatmap_weighted.png"
fig.savefig(output_file, dpi=300, bbox_inches="tight")
print("Bild gespeichert als:", output_file)

plt.show()