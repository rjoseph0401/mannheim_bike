import os
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import osmnx as ox
import pandas as pd
from matplotlib import cm, colors
from matplotlib.ticker import LogLocator

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"

# Ordner mit stadtradeln_2024.xlsx. Override mit Umgebungsvariable DATA_INPUT_DIR.
INPUT_DIR = Path(os.environ.get("DATA_INPUT_DIR", DATA_DIR.parent / "download"))
INPUT_FILE = INPUT_DIR / "stadtradeln_2024.xlsx"

OUTPUT_FILE = DATA_DIR / "stadtradeln_2024_heatmap_robust.png"
SRC_CRS = "EPSG:25832"
SAMPLE_STEP_M = 250
MAX_SAMPLES = 15

G = ox.load_graphml(GRAPH_FILE)
edges = ox.graph_to_gdfs(G, nodes=False, edges=True)

df = pd.read_excel(INPUT_FILE)
df.columns = df.columns.str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
needed = {"x_start", "y_start", "x_end", "y_end", "number_of_matched_trips"}
if not needed.issubset(df.columns):
    raise ValueError(f"Erwartete Spalten fehlen. Gefunden: {list(df.columns)}")

for c in ["x_start", "y_start", "x_end", "y_end", "number_of_matched_trips"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

df = df.dropna(subset=["x_start", "y_start", "x_end", "y_end", "number_of_matched_trips"]).copy()
dist = np.hypot(df["x_end"] - df["x_start"], df["y_end"] - df["y_start"])
n_samples = (dist / SAMPLE_STEP_M).round().astype(int).clip(lower=2, upper=MAX_SAMPLES)

xs, ys, ws = [], [], []
for row, n in zip(df.itertuples(index=False), n_samples):
    n = int(n)
    x = np.linspace(row.x_start, row.x_end, n)
    y = np.linspace(row.y_start, row.y_end, n)
    w = np.full(n, row.number_of_matched_trips / n)
    xs.extend(x)
    ys.extend(y)
    ws.extend(w)

pts = gpd.GeoDataFrame(geometry=gpd.points_from_xy(xs, ys), crs=SRC_CRS)
if G.graph.get("crs"):
    pts = pts.to_crs(G.graph.get("crs"))

nearest = ox.distance.nearest_edges(G, X=pts.geometry.x.to_numpy(), Y=pts.geometry.y.to_numpy())

hits = {}
for edge, w in zip(nearest, ws):
    hits[edge] = hits.get(edge, 0) + w

fig, ax = ox.plot_graph(
    G, node_size=0, edge_color="gray", edge_linewidth=0.4, bgcolor="white", show=False, close=False, figsize=(14, 14)
)

if hits:
    vmax = max(hits.values())
    vmin = min(v for v in hits.values() if v > 0)
    norm, cmap = colors.LogNorm(vmin=vmin, vmax=vmax), cm.get_cmap("YlOrRd")
    for idx, trips in hits.items():
        geom = edges.loc[idx].geometry
        for line in ([geom] if geom.geom_type == "LineString" else geom.geoms):
            ax.plot(*line.xy, color=cmap(norm(trips)), linewidth=1.0 + 3.0 * norm(trips), alpha=0.9)
    cbar = fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax, fraction=0.03, pad=0.01)
    cbar.locator = LogLocator(base=10, subs=(1.0, 2.0, 5.0))
    cbar.update_ticks()
    cbar.set_label("Anzahl Fahrten (Stadtradeln 2024, log)")

ax.set_title("Stadtradeln 2024 als Heatmap auf Mannheim Graph")
fig.savefig(OUTPUT_FILE, dpi=300, bbox_inches="tight")
print("Bild gespeichert als:", OUTPUT_FILE)
print("Gematchte Kanten:", len(hits))
plt.show()
