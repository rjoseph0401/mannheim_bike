import osmnx as ox
import matplotlib.pyplot as plt
import pandas as pd
import ast
from pathlib import Path
from matplotlib import cm, colors

df_nextbike_merged = pd.read_csv("df_nextbike_merged_mit_routen.csv")
df_nextbike_merged["route_als_liste"] = df_nextbike_merged["route_als_liste"].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) and x.strip().startswith("[") else pd.NA
)

graph_file = Path("mannheim_bike.graphml")
if graph_file.exists():
    G = ox.load_graphml(graph_file)
else:
    G = ox.graph_from_place("Mannheim, Germany", network_type="bike", simplify=True)
    ox.save_graphml(G, graph_file)

edges = ox.graph_to_gdfs(G, nodes=False, edges=True).copy()
routes = df_nextbike_merged["route_als_liste"].dropna()

all_x, all_y, route_ids = [], [], []
for idx, route in enumerate(routes):
    if isinstance(route, (list, tuple)) and len(route) >= 2:
        for i in range(len(route) - 1):
            lon1, lat1 = route[i]
            lon2, lat2 = route[i + 1]
            all_x.append((lon1 + lon2) / 2)
            all_y.append((lat1 + lat2) / 2)
            route_ids.append(idx)

nearest = ox.distance.nearest_edges(G, X=all_x, Y=all_y) if all_x else []

fig, ax = ox.plot_graph(G, node_size=4, node_color="limegreen", edge_color="gray", edge_linewidth=0.4, bgcolor="white", show=False, close=False, figsize=(14, 14))

hits = {}
seen = set()
for rid, edge in zip(route_ids, nearest):
    key = (rid, edge[0], edge[1], edge[2])
    if key not in seen:
        seen.add(key)
        hits[edge] = hits.get(edge, 0) + 1

if hits:
    vmax = max(hits.values())
    norm = colors.Normalize(vmin=1, vmax=vmax)
    cmap = cm.get_cmap("YlOrRd")

    for (u, v, k), n in hits.items():
        geom = edges.loc[(u, v, k)].geometry
        line_list = [geom] if geom.geom_type == "LineString" else geom.geoms
        for line in line_list:
            ax.plot(*line.xy, color=cmap(norm(n)), linewidth=1.5 + 2.5 * n / vmax, alpha=0.9)

    cbar = fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax, fraction=0.03, pad=0.01)
    cbar.set_label("Anzahl gematchter Routen")

ax.set_title("Nextbike-Routen als Heatmap auf Mannheim Graph mit OSMR")
output_file = "mannheim_nextbike_heatmap.png"
fig.savefig(output_file, dpi=300, bbox_inches="tight")
print("Bild gespeichert als:", output_file)
plt.show()
