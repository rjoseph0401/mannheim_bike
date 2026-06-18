from pathlib import Path
import ast

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import osmnx as ox
from matplotlib import cm
from matplotlib.colors import LogNorm
from collections import Counter

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
INPUT_FILE = DATA_DIR / "df_nextbike_merged_mit_routen.csv"
GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"
PERIOD_WINDOWS = [
    ("05-01", "06-01", "Mai-Juni"),
    ("07-25", "08-25", "Juli-August"),
]


def parse_route(value):
    if not isinstance(value, str) or not value.strip().startswith("["):
        return None
    try:
        coords = ast.literal_eval(value)
        return coords if isinstance(coords, list) and len(coords) > 1 else None
    except Exception:
        return None


def period_pairs(df, start, end):
    p = df[(df["rueckgabe_datetime"] >= pd.Timestamp(start)) & (df["rueckgabe_datetime"] < pd.Timestamp(end))].copy()
    p["coords"] = p["route_als_liste"].apply(parse_route)
    p = p[p["coords"].notna()].copy()
    return p


def compute_edge_hits(routes, graph):
    all_x, all_y, route_keys = [], [], []

    for idx, coords in enumerate(routes):
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            for a, b in zip(coords, coords[1:]):
                all_x.append((a[0] + b[0]) / 2)
                all_y.append((a[1] + b[1]) / 2)
                route_keys.append(idx)

    if not all_x:
        return Counter()

    nearest = ox.distance.nearest_edges(graph, X=all_x, Y=all_y)

    hits = Counter()
    seen = set()
    for route_idx, edge in zip(route_keys, nearest):
        key = (route_idx, edge[0], edge[1], edge[2])
        if key not in seen:
            seen.add(key)
            hits[edge] += 1

    return hits


def draw_period(ax, graph, edges, hits, title, period_text, trip_count, norm, cmap, vmax):
    if not hits:
        ax.set_title(f"{title} (keine Daten)")
        return

    ox.plot_graph(
        graph,
        node_size=0,
        edge_color="gray",
        edge_linewidth=0.4,
        bgcolor="white",
        show=False,
        close=False,
        figsize=(8, 8),
        ax=ax,
    )

    for (u, v, k), n in sorted(hits.items(), key=lambda item: item[1]):
        geom = edges.loc[(u, v, k)].geometry
        line_list = [geom] if geom.geom_type == "LineString" else geom.geoms
        for line in line_list:
            ax.plot(*line.xy, color=cmap(norm(n)), linewidth=1.2 + 2.0 * np.sqrt(n / vmax), alpha=0.9)

    ax.set_title(f"{title}\n{period_text} | Fahrten: {trip_count}")


df = pd.read_csv(
    INPUT_FILE,
    usecols=["rueckgabe_datetime", "route_als_liste"],
)
df["rueckgabe_datetime"] = pd.to_datetime(df["rueckgabe_datetime"], errors="coerce")
df = df.dropna(subset=["rueckgabe_datetime"]).copy()

target_year = int(df["rueckgabe_datetime"].dt.year.mode().iloc[0])
PERIODS = [
    (f"{target_year}-{s}", f"{target_year}-{e}", f"{label} {target_year}")
    for s, e, label in PERIOD_WINDOWS
]
print("Vergleichsjahr:", target_year)

if GRAPH_FILE.exists():
    graph = ox.load_graphml(GRAPH_FILE)
else:
    graph = ox.graph_from_place("Mannheim, Germany", network_type="bike", simplify=True)
    ox.save_graphml(graph, GRAPH_FILE)

edges = ox.graph_to_gdfs(graph, nodes=False, edges=True).copy()

left_period = period_pairs(df, PERIODS[0][0], PERIODS[0][1])
right_period = period_pairs(df, PERIODS[1][0], PERIODS[1][1])
left_routes = left_period["coords"]
right_routes = right_period["coords"]
left_trip_count = int(len(left_period))
right_trip_count = int(len(right_period))

print(f"Perioden: {len(left_routes)} vs {len(right_routes)} Routen")

left_hits = compute_edge_hits(left_routes, graph)
right_hits = compute_edge_hits(right_routes, graph)
all_vals = np.array(list(left_hits.values()) + list(right_hits.values()), dtype=float)
if len(all_vals) == 0:
    vmin, vmax = 1, 2
else:
    vmin = max(1, int(np.percentile(all_vals, 5)))
    vmax = int(np.percentile(all_vals, 99.5))
    vmax = max(vmax, vmin + 1)
norm = LogNorm(vmin=vmin, vmax=vmax)
cmap = plt.colormaps.get_cmap("turbo")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(19, 8))

draw_period(ax1, graph, edges, left_hits, PERIODS[0][2], f"{PERIODS[0][0]} bis {PERIODS[0][1]}", left_trip_count, norm, cmap, vmax)
draw_period(ax2, graph, edges, right_hits, PERIODS[1][2], f"{PERIODS[1][0]} bis {PERIODS[1][1]}", right_trip_count, norm, cmap, vmax)

fig.subplots_adjust(left=0.04, right=0.90, wspace=0.08, top=0.92, bottom=0.04)
cax = fig.add_axes([0.92, 0.14, 0.015, 0.72])
cbar = fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), cax=cax)
cbar.set_label("Anzahl gematchter Fahrten je Kante")

out = DATA_DIR / "nextbike_compare_heatmap_2024_periods_edges.jpg"
fig.savefig(out, dpi=150, bbox_inches="tight")
plt.close(fig)

print(f"Gespeichert: {out}")
