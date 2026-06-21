import osmnx as ox
import matplotlib.pyplot as plt
import pandas as pd
import ast
import numpy as np
from pathlib import Path
from matplotlib import cm, colors
import scipy.sparse as sp
import networkx as nx
import geopandas as gpd

print("Start of Code")

# --- Daten laden ---
#df = pd.read_csv("df_nextbike_merged_mit_routen.csv")  #Hier momentan die Daten von Radek enthalten, also OSRM

#Geodataframe 2024 Daten laden

gdf = gpd.read_file("stadtradeln2023_osrm_routes.geojson")

gdf["route_als_liste"] = gdf["geometry"].apply(
    lambda geom: list(geom.coords) if geom is not None else []
)
df = pd.DataFrame(gdf)

'''
df["route_als_liste"] = df["route_als_liste"].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) and x.strip().startswith("[") else pd.NA
)
'''

graph_file = Path("mannheim_bike.graphml")
graph_file_large = Path("mannheim_bike_large.graphml")

# Kleiner Graph (Mannheim exakt) – nur für finales Clipping
if graph_file.exists():
    G_mannheim = ox.load_graphml(graph_file)
else:
    G_mannheim = ox.graph_from_place("Mannheim, Germany", network_type="bike", simplify=True)
    ox.save_graphml(G_mannheim, graph_file)

# Großer Graph (mit Puffer) – fürs Edge-Matching der außenliegenden Routen
if graph_file_large.exists():
    G = ox.load_graphml(graph_file_large)
else:
    # Puffer: ~5km um Mannheim-Zentrum (Koordinaten anpassen falls nötig)
    G = ox.graph_from_point(
        center_point=(49.4875, 8.4660),
        dist=15000,                      # 15 km Radius – nach Bedarf anpassen
        network_type="bike",
        simplify=True
    )
    ox.save_graphml(G, graph_file_large)

# --- Kanten als GDF mit MultiIndex für schnellen Lookup ---
edges = ox.graph_to_gdfs(G, nodes=False, edges=True)

routes = df["route_als_liste"].dropna()

# --- Edge-Matching mit CSV-Cache ---
cache_file = Path("edge_matching_cache_Stadtradeln2023.csv")

if cache_file.exists():
    print("Lade gecachte Edge-Matches...")
    cache_df = pd.read_csv(cache_file)
    nearest = list(zip(cache_df["u"], cache_df["v"], cache_df["k"]))
    route_ids = cache_df["route_id"].tolist()
else:
    coords = []
    route_ids = []

    for idx, route in enumerate(routes):
        if isinstance(route, (list, tuple)) and len(route) >= 2:
            arr = np.array(route)
            midpoints = (arr[:-1] + arr[1:]) / 2
            coords.append(midpoints)
            route_ids.extend([idx] * len(midpoints))

    if coords:
        all_coords = np.vstack(coords)
        all_x = all_coords[:, 0]
        all_y = all_coords[:, 1]

        print(f"Matching {len(all_x)} Mittelpunkte zu Kanten...")
        nearest = [tuple(e) for e in ox.distance.nearest_edges(G, X=all_x, Y=all_y)]
        #nearest = ox.distance.nearest_edges(G, X=all_x, Y=all_y)

        cache_df = pd.DataFrame(nearest, columns=["u", "v", "k"])
        cache_df["route_id"] = route_ids
        cache_df.to_csv(cache_file, index=False)
        print("Cache gespeichert als:", cache_file)
    else:
        nearest = []

print("End of Route-to-Edge-Matching")

# --- Hits zählen ---
hits = {}
seen = set()
for rid, edge in zip(route_ids, nearest):
    key = (rid, edge[0], edge[1], edge[2])
    if key not in seen:
        seen.add(key)
        hits[edge] = hits.get(edge, 0) + 1

# --- Plot ---
fig, ax = ox.plot_graph(
    G, node_size=4, node_color="limegreen",
    edge_color="gray", edge_linewidth=0.4,
    bgcolor="white", show=False, close=False, figsize=(14, 14)
)

if hits:
    vmax = max(hits.values())
    norm = colors.Normalize(vmin=1, vmax=vmax)
    cmap = cm.get_cmap("YlOrRd")

    edge_geom = edges["geometry"].to_dict()

    for (u, v, k), n in hits.items():
        geom = edge_geom.get((u, v, k))
        if geom is None:
            continue
        line_list = [geom] if geom.geom_type == "LineString" else list(geom.geoms)
        color = cmap(norm(n))
        lw = 1.5 + 2.5 * n / vmax
        for line in line_list:
            ax.plot(*line.xy, color=color, linewidth=lw, alpha=0.9)

    cbar = fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax, fraction=0.03, pad=0.01)
    cbar.set_label("Anzahl gematchter Routen")

ax.set_title("Nextbike-Routen als Heatmap auf Mannheim Graph mit OSMR")
output_file = "mannheim_Stadtradeln2023_heatmapOSMR.png"
fig.savefig(output_file, dpi=300, bbox_inches="tight")
print("Bild gespeichert als:", output_file)
plt.show()

if not cache_file.exists():
    raise FileNotFoundError("Cache nicht gefunden. Bitte zuerst Graphmetriken.py ausführen.") #Cache wird in diesem Skript auch erstellt

cache_df = pd.read_csv(cache_file)
print(f"Cache geladen: {len(cache_df)} Einträge")

# --- Kanten ohne Route entfernen (auf großem Graph) ---
used_edges = set(zip(cache_df["u"], cache_df["v"], cache_df["k"]))

edges_to_remove = [
    (u, v, k) for u, v, k in G.edges(keys=True)
    if (u, v, k) not in used_edges
]
G.remove_edges_from(edges_to_remove)
isolated_nodes = list(nx.isolates(G))
G.remove_nodes_from(isolated_nodes)
print(f"Nach Route-Filter: {G.number_of_nodes()} Knoten, {G.number_of_edges()} Kanten")

# --- Clipping auf Mannheimer Stadtgebiet ---
print("Clippe auf Mannheimer Stadtgebiet...")

# Mannheim-Polygon laden
mannheim_boundary = ox.geocode_to_gdf("Mannheim, Germany")
mannheim_poly = mannheim_boundary.geometry.iloc[0]

# Knoten-GDF des großen Graphen
nodes_gdf, _ = ox.graph_to_gdfs(G)

# Nur Knoten innerhalb Mannheims behalten
nodes_within = nodes_gdf[nodes_gdf.geometry.within(mannheim_poly)]
nodes_to_keep = set(nodes_within.index)

# Knoten außerhalb entfernen
nodes_outside = [n for n in G.nodes() if n not in nodes_to_keep]
G.remove_nodes_from(nodes_outside)
print(f"Entfernte Knoten außerhalb Mannheims: {len(nodes_outside)}")

# Kanten, die durch das Entfernen isoliert wurden
isolated_nodes = list(nx.isolates(G))
G.remove_nodes_from(isolated_nodes)
print(f"Verbleibende Knoten (Mannheim): {G.number_of_nodes()}")
print(f"Verbleibende Kanten (Mannheim): {G.number_of_edges()}")

# cache_df ebenfalls auf verbleibende Knoten filtern
remaining_nodes = set(G.nodes())
cache_df = cache_df[
    cache_df["u"].isin(remaining_nodes) & cache_df["v"].isin(remaining_nodes)
]

# --- Node-Index erstellen ---
nodes = sorted(G.nodes())
node_index = {node: i for i, node in enumerate(nodes)}
n = len(nodes)

# --- Adjacency Matrix (sparse) ---
edge_set = set(zip(cache_df["u"], cache_df["v"], cache_df["k"]))

rows, cols = [], []
for (u, v, k) in edge_set:
    if u in node_index and v in node_index:
        i, j = node_index[u], node_index[v]
        rows.append(i); cols.append(j)
        rows.append(j); cols.append(i)

adj_matrix = sp.csr_matrix(
    (np.ones(len(rows), dtype=np.int8), (rows, cols)),
    shape=(n, n)
)

print(f"Adjacency Matrix Größe: {n} x {n}")
print(f"Nicht-Null-Einträge: {adj_matrix.nnz}")

# --- Speichern ---
sp.save_npz("adjacency_matrixStadtradeln2023.npz", adj_matrix)
print("Sparse Matrix gespeichert als: adjacency_matrixStadtradeln2023.npz")

pd.DataFrame({"osm_id": nodes, "matrix_index": range(n)}).to_csv("node_indexStadtradeln2023.csv", index=False)
print("Node-Index gespeichert als: node_indexStadtradeln2023.csv")

if n <= 5000:
    dense = pd.DataFrame(adj_matrix.toarray(), index=nodes, columns=nodes)
    dense.to_csv("adjacency_matrix_denseStadtradeln2023.csv")
    print("Dense Matrix gespeichert als: adjacency_matrix_denseStadtradeln2023.csv")
else:
    print(f"Graph zu groß ({n} Knoten) für dense CSV — nur sparse gespeichert.")