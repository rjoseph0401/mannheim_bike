import osmnx as ox
import matplotlib.pyplot as plt
import pandas as pd
import ast
import numpy as np
import json
from pathlib import Path
from matplotlib import cm, colors
from matplotlib.colors import LogNorm
from concurrent.futures import ThreadPoolExecutor

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/

df_nextbike_merged = pd.read_csv(DATA_DIR / "df_nextbike_merged_mit_routen.csv")
df_nextbike_merged["route_als_liste"] = df_nextbike_merged["route_als_liste"].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) and x.strip().startswith("[") else pd.NA
)

graph_file = DATA_DIR / "mannheim_bike.graphml"
if graph_file.exists():
    G = ox.load_graphml(graph_file)
else:
    G = ox.graph_from_place("Mannheim, Germany", network_type="bike", simplify=True)
    ox.save_graphml(G, graph_file)

edges = ox.graph_to_gdfs(G, nodes=False, edges=True).copy()
routes = df_nextbike_merged["route_als_liste"].dropna()
hits_cache_file = DATA_DIR / "cache" / "nextbike_edge_hits_cache.json"

hits = {}
if hits_cache_file.exists():
    try:
        cache = json.loads(hits_cache_file.read_text(encoding="utf-8"))
        hits = {(u, v, k): int(c) for u, v, k, c in cache.get("hits", [])}
        print(f"Nutze Nextbike-Hits aus Cache: {len(hits)} Kanten")
    except Exception:
        hits = {}

if not hits:
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

    def aggregate_chunk(chunk_data):
        chunk_dict = {}
        for _, edge in chunk_data:
            chunk_dict[edge] = chunk_dict.get(edge, 0) + 1
        return chunk_dict

    data = list(zip(route_ids, nearest))
    seen = set()
    data_unique = []
    for rid, edge in data:
        key = (rid, edge[0], edge[1], edge[2])
        if key not in seen:
            seen.add(key)
            data_unique.append((rid, edge))

    chunk_size = max(1000, len(data_unique) // 8)
    with ThreadPoolExecutor(max_workers=128) as ex:
        futures = [ex.submit(aggregate_chunk, data_unique[i:i+chunk_size]) for i in range(0, len(data_unique), chunk_size)]
        for fut in futures:
            chunk_dict = fut.result()
            for edge, count in chunk_dict.items():
                hits[edge] = hits.get(edge, 0) + count

    hits_cache_file.parent.mkdir(exist_ok=True)
    payload = {"hits": [[u, v, k, int(c)] for (u, v, k), c in hits.items()]}
    hits_cache_file.write_text(json.dumps(payload), encoding="utf-8")
    print(f"Nextbike-Hits Cache gespeichert: {hits_cache_file}")

fig, ax = ox.plot_graph(G, node_size=0, edge_color="gray", edge_linewidth=0.4, bgcolor="white", show=False, close=False, figsize=(14, 14))

if hits:
    vals = np.array(list(hits.values()))
    vmin = max(1, int(np.percentile(vals, 5)))
    vmax = int(np.percentile(vals, 99.5))
    vmax = max(vmax, vmin + 1)
    norm = LogNorm(vmin=vmin, vmax=vmax)
    cmap = cm.get_cmap("turbo")

    for (u, v, k), n in sorted(hits.items(), key=lambda item: item[1]):
        geom = edges.loc[(u, v, k)].geometry
        line_list = [geom] if geom.geom_type == "LineString" else geom.geoms
        for line in line_list:
            ax.plot(*line.xy, color=cmap(norm(n)), linewidth=1.2 + 2.0 * np.sqrt(n / vmax), alpha=0.9)

    cbar = fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax, fraction=0.03, pad=0.01)
    cbar.set_label("Anzahl gematchter Routen")

ax.set_title("Nextbike-Routen als Heatmap auf Mannheim Graph mit OSMR")
output_file = DATA_DIR / "mannheim_nextbike_heatmap.png"
fig.savefig(output_file, dpi=300, bbox_inches="tight")
print("Bild gespeichert als:", output_file)
plt.show()
