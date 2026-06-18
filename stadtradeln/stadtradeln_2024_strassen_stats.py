import os
from pathlib import Path

import geopandas as gpd
import numpy as np
import osmnx as ox
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
GRAPH_FILE = DATA_DIR / "mannheim_bike_hauptachsen.graphml"

# Ordner mit stadtradeln_2024.xlsx. Override mit Umgebungsvariable DATA_INPUT_DIR.
INPUT_DIR = Path(os.environ.get("DATA_INPUT_DIR", DATA_DIR.parent / "download"))
INPUT_FILE = INPUT_DIR / "stadtradeln_2024.xlsx"

OUTPUT_FILE = DATA_DIR / "stadtradeln_2024_strassen_stats_hauptachsen.csv"
SRC_CRS = "EPSG:25832"
SAMPLE_STEP_M = 250
MAX_SAMPLES = 15


def street_name(value):
    if isinstance(value, (list, tuple, set)):
        return " / ".join(str(v) for v in value if str(v).strip()) or "(ohne Namen)"
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
        value = value.tolist()
        if isinstance(value, (list, tuple, set)):
            return " / ".join(str(v) for v in value if str(v).strip()) or "(ohne Namen)"
    try:
        if pd.isna(value):
            return "(ohne Namen)"
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text if text else "(ohne Namen)"


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
    xs.extend(np.linspace(row.x_start, row.x_end, n))
    ys.extend(np.linspace(row.y_start, row.y_end, n))
    ws.extend(np.full(n, row.number_of_matched_trips / n))

pts = gpd.GeoDataFrame(geometry=gpd.points_from_xy(xs, ys), crs=SRC_CRS)
if G.graph.get("crs"):
    pts = pts.to_crs(G.graph.get("crs"))

nearest = ox.distance.nearest_edges(G, X=pts.geometry.x.to_numpy(), Y=pts.geometry.y.to_numpy())

edge_hits = {}
for edge, w in zip(nearest, ws):
    edge_hits[edge] = edge_hits.get(edge, 0) + w

edge_df = pd.DataFrame(
    [{"u": u, "v": v, "key": k, "trips": trips} for (u, v, k), trips in edge_hits.items()]
)
edge_df = edge_df.join(edges[["name", "length"]], on=["u", "v", "key"], how="left")
edge_df["street"] = edge_df["name"].apply(street_name)

stats = (
    edge_df.groupby("street", as_index=False)
    .agg(
        trips_sum=("trips", "sum"),
        kanten=("street", "size"),
        netzlaenge_m=("length", "sum"),
    )
    .sort_values("trips_sum", ascending=False)
)

stats["trips_pro_kante"] = stats["trips_sum"] / stats["kanten"].clip(lower=1)
stats["trips_pro_km"] = stats["trips_sum"] / (stats["netzlaenge_m"].clip(lower=1) / 1000)

stats = stats.sort_values("trips_sum", ascending=False)

stats.to_csv(OUTPUT_FILE, index=False)
print("Gespeichert:", OUTPUT_FILE)
print("\nTop 20 meistbefahrene Straßen:")
print(stats.head(20).round(2).to_string(index=False))
