import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import osmnx as ox
import pandas as pd
from matplotlib import cm
from matplotlib.colors import LogNorm

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"
HITS_CACHE_FILE = DATA_DIR / "cache" / "nextbike_edge_hits_cache.json"
OUT_FILE = DATA_DIR / "nextbike_heatmap_vergleich_radfreundlich.png"


def has_value(value):
    if pd.isna(value):
        return False
    text = str(value).strip().lower()
    return text not in {"", "none", "nan"}


def to_rows(edges, hits):
    rows = []
    for key, count in hits.items():
        if key not in edges.index:
            continue
        edge = edges.loc[key]
        rows.append(
            {
                "edge_key": key,
                "count": int(count),
                "geometry": edge.geometry,
                "bicycle": edge.get("bicycle", pd.NA),
                "cycleway": edge.get("cycleway", pd.NA),
                "surface": edge.get("surface", pd.NA),
            }
        )
    return pd.DataFrame(rows)


def draw_heatmap(ax, graph, data, norm, cmap, title):
    ox.plot_graph(
        graph,
        ax=ax,
        node_size=0,
        edge_color="#c7c7c7",
        edge_linewidth=0.4,
        bgcolor="white",
        show=False,
        close=False,
    )

    if data.empty:
        ax.set_title(f"{title}\n(keine Treffer)")
        return

    vmax = max(1.0, float(data["count"].max()))
    for row in data.sort_values("count", ascending=True).itertuples(index=False):
        n = max(float(row.count), float(norm.vmin))
        geom = row.geometry
        line_list = [geom] if geom.geom_type == "LineString" else list(geom.geoms)
        for line in line_list:
            ax.plot(
                *line.xy,
                color=cmap(norm(n)),
                linewidth=1.2 + 2.0 * np.sqrt(float(row.count) / vmax),
                alpha=0.9,
            )

    ax.set_title(title)


def main():
    if not GRAPH_FILE.exists() or not HITS_CACHE_FILE.exists():
        raise FileNotFoundError("Benötigt: mannheim_bike.graphml und cache/nextbike_edge_hits_cache.json")

    graph = ox.load_graphml(GRAPH_FILE)
    edges = ox.graph_to_gdfs(graph, nodes=False, edges=True).copy()

    cache = json.loads(HITS_CACHE_FILE.read_text(encoding="utf-8"))
    hits = {(u, v, k): c for u, v, k, c in cache.get("hits", [])}

    df = to_rows(edges, hits)
    if df.empty:
        raise ValueError("Keine verwertbaren Hits gefunden.")

    tag_mask = (
        df["bicycle"].apply(has_value)
        | df["cycleway"].apply(has_value)
        | df["surface"].apply(has_value)
    )
    df_friendly = df[tag_mask].copy()

    vals = df["count"].to_numpy(dtype=float)
    vmin = max(1.0, float(np.percentile(vals, 5)))
    vmax = max(vmin + 1.0, float(np.percentile(vals, 99.5)))
    norm = LogNorm(vmin=vmin, vmax=vmax)
    cmap = cm.get_cmap("turbo")

    fig, axes = plt.subplots(1, 2, figsize=(18, 9))
    draw_heatmap(axes[0], graph, df, norm, cmap, "Nextbike Heatmap: alle gematchten Kanten")
    draw_heatmap(
        axes[1],
        graph,
        df_friendly,
        norm,
        cmap,
        "Nextbike Heatmap: nur Kanten mit bicycle/cycleway/surface",
    )

    sm = cm.ScalarMappable(norm=norm, cmap=cmap)
    cbar = fig.colorbar(sm, ax=axes.ravel().tolist(), fraction=0.03, pad=0.02)
    cbar.set_label("Anzahl gematchter Routen")

    plt.tight_layout()
    fig.savefig(OUT_FILE, dpi=300, bbox_inches="tight")

    print(f"Gespeichert: {OUT_FILE}")
    print(f"Alle Kanten: {len(df)}")
    print(f"Radfreundlich getaggt: {len(df_friendly)}")
    print(f"Anteil: {100.0 * len(df_friendly) / len(df):.2f}%")


if __name__ == "__main__":
    main()
