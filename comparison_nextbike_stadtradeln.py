import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
plt.ioff()

import osmnx as ox
from pathlib import Path
from matplotlib import cm, colors
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

# ============================================================
# Dateien
# ============================================================

DATA_DIR = Path("Data")

GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"

STADT_GPKG = DATA_DIR / "stadtradeln_graphhopper_routes.gpkg"
STADT_LAYER = "routes"

NEXT_GPKG = DATA_DIR / "nextbike_graphhopper_routes.gpkg"
NEXT_LAYER = "routes"

OUT_STADT = DATA_DIR / "vergleich_stadtradeln_heatmap.png"
OUT_NEXT = DATA_DIR / "vergleich_nextbike_heatmap.png"
OUT_DIFF = DATA_DIR / "vergleich_diff_stadtradeln_minus_nextbike.png"
OUT_CSV = DATA_DIR / "vergleich_edge_stats.csv"

# ============================================================
# Parameter
# ============================================================

MAX_MATCH_DIST = 40
LINEWIDTH_MIN = 0.8
LINEWIDTH_MAX = 3.2
VMIN_PERCENTILE = 5
VMAX_PERCENTILE = 99.5

# ============================================================
# Graph laden / projizieren
# ============================================================

if GRAPH_FILE.exists():
    G = ox.load_graphml(GRAPH_FILE)
else:
    G = ox.graph_from_place("Mannheim, Germany", network_type="bike", simplify=True)
    ox.save_graphml(G, GRAPH_FILE)

G_proj = ox.project_graph(G)
edges_proj = ox.graph_to_gdfs(G_proj, nodes=False, edges=True).copy()
graph_crs = edges_proj.crs

print("Graph geladen.")
print("Graph CRS:", graph_crs)
print(f"Kanten im Graph: {len(edges_proj):,}")

# ============================================================
# Hilfsfunktionen
# ============================================================

def load_routes_from_gpkg(gpkg_file, layer, weight_col):
    gdf = gpd.read_file(gpkg_file, layer=layer)
    gdf = gdf.dropna(subset=["geometry"]).copy()
    gdf = gdf[gdf.geometry.geom_type.isin(["LineString", "MultiLineString"])].copy()

    if weight_col not in gdf.columns:
        raise ValueError(f"Spalte '{weight_col}' fehlt in {gpkg_file} / Layer {layer}")

    gdf[weight_col] = pd.to_numeric(gdf[weight_col], errors="coerce").fillna(1).astype(float)
    gdf = gdf.to_crs(graph_crs).copy()
    return gdf


def build_edge_hits(gdf_routes, weight_col, label):
    all_x, all_y, route_ids = [], [], []
    total_routes = len(gdf_routes)

    print(f"\n--- {label}: Segment-Mittelpunkte berechnen ---")
    for rid, row in enumerate(gdf_routes.itertuples()):
        if rid % 100 == 0 and total_routes > 0:
            print(f"{label}: Routen {rid}/{total_routes} ({rid/total_routes:.1%})", end="\r")

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

    print(f"{label}: Routen {total_routes}/{total_routes} (100%)")
    print(f"{label}: Segmente gesamt: {len(route_ids):,}")

    print(f"{label}: nearest_edges ...")
    nearest = ox.distance.nearest_edges(G_proj, X=all_x, Y=all_y) if all_x else []
    print(f"{label}: nearest_edges abgeschlossen.")

    weights = gdf_routes[weight_col].tolist()
    hits = {}
    seen = set()
    discarded_far = 0
    total_edges = len(route_ids)

    print(f"--- {label}: Edge-Matching ---")
    for i, (rid, edge, x, y) in enumerate(zip(route_ids, nearest, all_x, all_y)):
        if i % 5000 == 0 and total_edges > 0:
            print(f"{label}: Matching {i}/{total_edges} ({i/total_edges:.1%})", end="\r")

        edge_key = (edge[0], edge[1], edge[2])
        dedup_key = (rid, edge[0], edge[1], edge[2])

        if dedup_key in seen:
            continue

        geom = edges_proj.loc[edge_key].geometry
        dist = geom.distance(Point(x, y))

        if dist > MAX_MATCH_DIST:
            discarded_far += 1
            continue

        seen.add(dedup_key)
        hits[edge_key] = hits.get(edge_key, 0.0) + float(weights[rid])

    if total_edges > 0:
        print(f"{label}: Matching {total_edges}/{total_edges} (100%)")

    print(f"{label}: Gematchte Kanten: {len(hits):,}")
    print(f"{label}: Verworfene Fern-Matches (> {MAX_MATCH_DIST} m): {discarded_far:,}")

    vals = np.array(list(hits.values())) if hits else np.array([])
    if len(vals) > 0:
        print(f"{label}: Hit-Range min={vals.min():.3f}, max={vals.max():.3f}")

    return hits


def normalize_hits(hits):
    total = sum(hits.values())
    if total <= 0:
        return {k: 0.0 for k in hits}, 0.0
    return {k: v / total for k, v in hits.items()}, total


def concentration_index(hits_norm):
    vals = np.array(list(hits_norm.values()), dtype=float)
    if len(vals) == 0:
        return 0.0
    return float(np.sum(vals ** 2))


def plot_heatmap_from_hits(hits, title, cbar_label, output_file):
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

    vals = np.array(list(hits.values()), dtype=float)
    vmin = max(1.0, float(np.percentile(vals, VMIN_PERCENTILE)))
    vmax = float(np.percentile(vals, VMAX_PERCENTILE))
    vmax = max(vmax, vmin + 1.0)

    norm = colors.LogNorm(vmin=vmin, vmax=vmax)
    cmap = cm.get_cmap("turbo")

    for (u, v, k), n in hits.items():
        geom = edges_proj.loc[(u, v, k)].geometry
        line_list = [geom] if geom.geom_type == "LineString" else list(geom.geoms)

        n_plot = min(float(n), vmax)
        line_width = LINEWIDTH_MIN + (LINEWIDTH_MAX - LINEWIDTH_MIN) * (n_plot / vmax)

        for line in line_list:
            ax.plot(
                *line.xy,
                color=cmap(norm(n_plot)),
                linewidth=line_width,
                alpha=0.9,
                solid_capstyle="round",
            )

    cbar = fig.colorbar(
        cm.ScalarMappable(norm=norm, cmap=cmap),
        ax=ax,
        fraction=0.03,
        pad=0.01,
    )
    cbar.set_label(cbar_label)

    ax.set_title(title)
    fig.savefig(output_file, dpi=300, bbox_inches="tight")
    print("Bild gespeichert als:", output_file)
    plt.close(fig)


def plot_diff_map(diff_hits, title, output_file):
    fig, ax = ox.plot_graph(
        G_proj,
        node_size=0,
        node_color="none",
        edge_color="lightgray",
        edge_linewidth=0.35,
        bgcolor="white",
        show=False,
        close=False,
        figsize=(14, 14),
    )

    vals = np.array(list(diff_hits.values()), dtype=float)
    max_abs = float(np.percentile(np.abs(vals), 99.5))
    max_abs = max(max_abs, 1e-12)

    norm = colors.TwoSlopeNorm(vmin=-max_abs, vcenter=0.0, vmax=max_abs)
    cmap = cm.get_cmap("coolwarm")

    for (u, v, k), d in diff_hits.items():
        if abs(d) <= 0:
            continue

        geom = edges_proj.loc[(u, v, k)].geometry
        line_list = [geom] if geom.geom_type == "LineString" else list(geom.geoms)

        d_plot = max(-max_abs, min(max_abs, float(d)))
        width_strength = abs(d_plot) / max_abs
        line_width = 0.8 + 3.0 * width_strength

        for line in line_list:
            ax.plot(
                *line.xy,
                color=cmap(norm(d_plot)),
                linewidth=line_width,
                alpha=0.95,
                solid_capstyle="round",
            )

    cbar = fig.colorbar(
        cm.ScalarMappable(norm=norm, cmap=cmap),
        ax=ax,
        fraction=0.03,
        pad=0.01,
    )
    cbar.set_label("Differenz normierter Kantenanteile (Stadtradeln - Nextbike)")

    ax.set_title(title)
    fig.savefig(output_file, dpi=300, bbox_inches="tight")
    print("Bild gespeichert als:", output_file)
    plt.close(fig)


# ============================================================
# Datensätze laden
# ============================================================

gdf_stadt = load_routes_from_gpkg(STADT_GPKG, STADT_LAYER, weight_col="trips")
gdf_next = load_routes_from_gpkg(NEXT_GPKG, NEXT_LAYER, weight_col="count")

print(f"\nStadtradeln-Routen: {len(gdf_stadt):,}")
print(f"Nextbike-Routen:    {len(gdf_next):,}")

# ============================================================
# Hits auf Kantenebene bauen
# ============================================================

hits_stadt = build_edge_hits(gdf_stadt, weight_col="trips", label="Stadtradeln")
hits_next = build_edge_hits(gdf_next, weight_col="count", label="Nextbike")

# ============================================================
# Normalisieren
# ============================================================

hits_stadt_norm, total_stadt = normalize_hits(hits_stadt)
hits_next_norm, total_next = normalize_hits(hits_next)

print("\n--- Summen / Normalisierung ---")
print(f"Stadtradeln Gesamtsumme: {total_stadt:,.3f}")
print(f"Nextbike Gesamtsumme:    {total_next:,.3f}")

# ============================================================
# Vergleichstabelle pro Kante
# ============================================================

all_edges = sorted(set(hits_stadt.keys()) | set(hits_next.keys()))

rows = []
for edge in all_edges:
    st_abs = float(hits_stadt.get(edge, 0.0))
    nb_abs = float(hits_next.get(edge, 0.0))
    st_norm = float(hits_stadt_norm.get(edge, 0.0))
    nb_norm = float(hits_next_norm.get(edge, 0.0))
    diff = st_norm - nb_norm

    rows.append({
        "u": edge[0],
        "v": edge[1],
        "key": edge[2],
        "stadtradeln_abs": st_abs,
        "nextbike_abs": nb_abs,
        "stadtradeln_norm": st_norm,
        "nextbike_norm": nb_norm,
        "diff_norm": diff,
    })

df_compare = pd.DataFrame(rows)

edge_geoms = edges_proj[["geometry"]].copy().reset_index()
df_compare = df_compare.merge(edge_geoms, on=["u", "v", "key"], how="left")
gdf_compare = gpd.GeoDataFrame(df_compare, geometry="geometry", crs=graph_crs)

gdf_compare.to_csv(OUT_CSV, index=False)
print("\nCSV gespeichert:", OUT_CSV)

# ============================================================
# Kennzahlen
# ============================================================

conc_stadt = concentration_index(hits_stadt_norm)
conc_next = concentration_index(hits_next_norm)

print("\n--- Konzentrationsmaße ---")
print(f"Stadtradeln Konzentration Σp²: {conc_stadt:.8f}")
print(f"Nextbike    Konzentration Σp²: {conc_next:.8f}")

top_stadt = gdf_compare.sort_values("stadtradeln_norm", ascending=False)[
    ["u", "v", "key", "stadtradeln_abs", "stadtradeln_norm"]
].head(20)

top_next = gdf_compare.sort_values("nextbike_norm", ascending=False)[
    ["u", "v", "key", "nextbike_abs", "nextbike_norm"]
].head(20)

top_diff_pos = gdf_compare.sort_values("diff_norm", ascending=False)[
    ["u", "v", "key", "stadtradeln_norm", "nextbike_norm", "diff_norm"]
].head(20)

top_diff_neg = gdf_compare.sort_values("diff_norm", ascending=True)[
    ["u", "v", "key", "stadtradeln_norm", "nextbike_norm", "diff_norm"]
].head(20)

print("\n--- Top 20 Stadtradeln-Kanten ---")
print(top_stadt.to_string(index=False))

print("\n--- Top 20 Nextbike-Kanten ---")
print(top_next.to_string(index=False))

print("\n--- Top 20 Stadtradeln > Nextbike ---")
print(top_diff_pos.to_string(index=False))

print("\n--- Top 20 Nextbike > Stadtradeln ---")
print(top_diff_neg.to_string(index=False))

# ============================================================
# Plots
# ============================================================

plot_heatmap_from_hits(
    hits_stadt,
    title="Stadtradeln-Routen (gewichtet nach trips) auf Mannheim-Bike-Graph",
    cbar_label="Gewichtete Häufigkeit (Summe trips)",
    output_file=OUT_STADT,
)

plot_heatmap_from_hits(
    hits_next,
    title="Nextbike-Routen (gewichtet nach count) auf Mannheim-Bike-Graph",
    cbar_label="Gewichtete Häufigkeit (Summe count)",
    output_file=OUT_NEXT,
)

diff_hits = {edge: hits_stadt_norm.get(edge, 0.0) - hits_next_norm.get(edge, 0.0) for edge in all_edges}

plot_diff_map(
    diff_hits,
    title="Differenzkarte: Stadtradeln - Nextbike (normierte Kantenanteile)",
    output_file=OUT_DIFF,
)

print("\nFertig.")