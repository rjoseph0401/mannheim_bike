"""
Karte der Top-N meistgefahrenen Stadtradeln-Routen je Jahr.
Plotstil analog zu stadtradeln_graph_generation.py (osmnx + matplotlib).
Straßennamen werden aus dem OSM-Graph abgeleitet.
"""

import time
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import osmnx as ox

try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path.cwd()

DATA_DIR = BASE_DIR / "Data"
OUT_DIR = BASE_DIR / "Results"
OUT_DIR.mkdir(exist_ok=True)

YEARS = [2022, 2023, 2024]
TOP_N = 15
PROJ_CRS = "EPSG:25832"
GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"

COLORS = [
    "#e63946", "#f4a261", "#2a9d8f", "#457b9d", "#6a4c93",
    "#e76f51", "#264653", "#a8dadc", "#f1c453", "#90be6d",
    "#43aa8b", "#577590", "#f9844a", "#4d908e", "#277da1",
]


def fmt_seconds(s: float) -> str:
    s = int(round(s))
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")


def edge_name(G, u, v, k=0) -> str:
    data = G.get_edge_data(u, v, k) or G.get_edge_data(v, u, k) or {}
    name = data.get("name", "")
    if isinstance(name, list):
        name = name[0]
    return str(name) if name else ""


def load_top(year: int) -> gpd.GeoDataFrame:
    path = DATA_DIR / "outputs" / f"stadtradeln_graphhopper_routes_{year}.gpkg"
    gdf = gpd.read_file(path).to_crs(PROJ_CRS)
    gdf["seg_len_m"] = gdf.geometry.length
    od = (
        gdf.groupby(["key", "slon", "slat", "elon", "elat"])
        .agg(route_len_m=("seg_len_m", "sum"), trips=("trips", "first"),
             geometry=("geometry", "first"))
        .reset_index()
    )
    return gpd.GeoDataFrame(od, geometry="geometry", crs=PROJ_CRS) \
               .nlargest(TOP_N, "trips") \
               .reset_index(drop=True)


# ── Graph laden ──────────────────────────────────────────────────────────────

print("Lade Graph...")
t0 = time.time()
if GRAPH_FILE.exists():
    G = ox.load_graphml(GRAPH_FILE)
else:
    G = ox.graph_from_place("Mannheim, Germany", network_type="bike", simplify=True)
    ox.save_graphml(G, GRAPH_FILE)

G_proj = ox.project_graph(G)
edges_proj = ox.graph_to_gdfs(G_proj, nodes=False, edges=True)
print(f"  Graph geladen ({fmt_seconds(time.time() - t0)})")

# ── Daten laden ──────────────────────────────────────────────────────────────

print("Lade Top-Routen...")
top_data = {}
for year in YEARS:
    top_data[year] = load_top(year)
    print(f"  {year}: {len(top_data[year])} Routen geladen")

# ── Straßennamen aus OSM-Graph ───────────────────────────────────────────────

print("Leite Straßennamen ab...")
for year in YEARS:
    top = top_data[year].copy()

    start_pts = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(top["slon"], top["slat"]), crs="EPSG:4326"
    ).to_crs(G_proj.graph["crs"])

    end_pts = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(top["elon"], top["elat"]), crs="EPSG:4326"
    ).to_crs(G_proj.graph["crs"])

    ne_start = ox.distance.nearest_edges(
        G_proj, X=start_pts.geometry.x.values, Y=start_pts.geometry.y.values
    )
    ne_end = ox.distance.nearest_edges(
        G_proj, X=end_pts.geometry.x.values, Y=end_pts.geometry.y.values
    )

    top["start_street"] = [edge_name(G_proj, u, v, k) for u, v, k in ne_start]
    top["end_street"] = [edge_name(G_proj, u, v, k) for u, v, k in ne_end]

    top_data[year] = top

# ── Plot je Jahr ─────────────────────────────────────────────────────────────

for year in YEARS:
    print(f"Zeichne Karte {year}...")
    top = top_data[year]

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

    max_trips = top["trips"].max()

    for _, row in top.iterrows():
        coords = np.array(row.geometry.coords)
        lw = 2.0 + 5.0 * (row.trips / max_trips)
        ax.plot(coords[:, 0], coords[:, 1],
                color="#e63946", linewidth=lw, alpha=0.85,
                solid_capstyle="round", zorder=3)

    ax.set_title(f"Stadtradeln {year} – Top {TOP_N} Segmente", fontsize=14)

    out = OUT_DIR / f"stadtradeln_top{TOP_N}_routen_{year}.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Gespeichert: {out.name}")

print("Fertig.")
