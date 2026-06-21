import osmnx as ox
import matplotlib.pyplot as plt
import pandas as pd
import ast
import numpy as np
from pathlib import Path
from matplotlib import cm, colors
import scipy.sparse as sp
import networkx as nx

print("Start of Code")

# --- Daten laden ---
df = pd.read_csv("Df_NextbikemitGraphhopperroutes.csv")  #Nextbike Fahrten mit Graphhopper Routen laden
df["route_als_liste"] = df["route_als_liste"].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) and x.strip().startswith("[") else pd.NA
)

# --- Graph laden ---
graph_file = Path("mannheim_bike.graphml")
if graph_file.exists():
    G = ox.load_graphml(graph_file)
else:
    G = ox.graph_from_place("Mannheim, Germany", network_type="bike", simplify=True)
    ox.save_graphml(G, graph_file)

# --- Kanten als GDF mit MultiIndex für schnellen Lookup ---
edges = ox.graph_to_gdfs(G, nodes=False, edges=True)

routes = df["route_als_liste"].dropna()

# --- Edge-Matching mit CSV-Cache ---
cache_file = Path("edge_matching_cacheGraphhopper.csv")         # Lade das Matching der Fahrten an die Kanten des Graphen

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

ax.set_title("Nextbike-Routen als Heatmap auf Mannheim Graph mit Graphhopper")
output_file = "mannheim_nextbike_heatmapGraphhopper.png"
fig.savefig(output_file, dpi=300, bbox_inches="tight")
print("Bild gespeichert als:", output_file)
plt.show()

cache_file = Path("edge_matching_cacheGraphhopper.csv")
if not cache_file.exists():
    raise FileNotFoundError("Cache nicht gefunden. Bitte zuerst Graphmetriken.py ausführen.") #Cache wird in diesem Skript auch erstellt

cache_df = pd.read_csv(cache_file)
print(f"Cache geladen: {len(cache_df)} Einträge")

# --- Kanten ohne Route entfernen ---
used_edges = set(zip(cache_df["u"], cache_df["v"], cache_df["k"]))

edges_to_remove = [
    (u, v, k) for u, v, k in G.edges(keys=True)
    if (u, v, k) not in used_edges
]
G.remove_edges_from(edges_to_remove)
print(f"Entfernte Kanten: {len(edges_to_remove)}")

# Isolierte Knoten (ohne Kanten) ebenfalls entfernen
isolated_nodes = list(nx.isolates(G))
G.remove_nodes_from(isolated_nodes)
print(f"Verbleibende Knoten: {G.number_of_nodes()}")
print(f"Verbleibende Kanten: {G.number_of_edges()}")

# --- Node-Index erstellen ---
nodes = sorted(G.nodes())
node_index = {node: i for i, node in enumerate(nodes)}
n = len(nodes)

# --- Adjacency Matrix (sparse) ---
rows, cols = [], []
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
# Als sparse Matrix (empfohlen bei großen Graphen)
sp.save_npz("adjacency_matrixGraphhopper.npz", adj_matrix)
print("Sparse Matrix gespeichert als: adjacency_matrixGraphhopper.npz")

# Node-Index speichern (um später OSM-IDs zuzuordnen)
pd.DataFrame({"osm_id": nodes, "matrix_index": range(n)}).to_csv("node_indexGraphhopper.csv", index=False)
print("Node-Index gespeichert als: node_indexGraphhopper.csv")

# Optional: Als dense CSV (nur bei kleinen Graphen sinnvoll!)
if n <= 5000:
    dense = pd.DataFrame(adj_matrix.toarray(), index=nodes, columns=nodes)
    dense.to_csv("adjacency_matrix_denseGraphhopper.csv")
    print("Dense Matrix gespeichert als: adjacency_matrix_denseGraphhopper.csv")
else:
    print(f"Graph zu groß ({n} Knoten) für dense CSV — nur sparse gespeichert.")