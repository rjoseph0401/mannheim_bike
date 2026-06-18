import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import osmnx as ox
from matplotlib import cm, colors

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"
HITS_CACHE_FILE = DATA_DIR / "cache" / "nextbike_edge_hits_cache.json"
OUT_FILE = DATA_DIR / "mannheim_nextbike_heatmap_osrm_anteil.png"


def main():
    if not GRAPH_FILE.exists() or not HITS_CACHE_FILE.exists():
        raise FileNotFoundError("Benötigt: mannheim_bike.graphml und cache/nextbike_edge_hits_cache.json")

    graph = ox.load_graphml(GRAPH_FILE)
    edges = ox.graph_to_gdfs(graph, nodes=False, edges=True).copy()

    cache = json.loads(HITS_CACHE_FILE.read_text(encoding="utf-8"))
    hits = {(u, v, k): int(c) for u, v, k, c in cache.get("hits", []) if (u, v, k) in edges.index}
    if not hits:
        raise ValueError("Keine Hits im Cache gefunden.")

    total = sum(hits.values())
    shares = {k: v / total for k, v in hits.items()}

    vals = np.array(list(shares.values()), dtype=float)
    vmin = max(float(np.percentile(vals, 1)), 1e-9)
    vmax = max(float(np.percentile(vals, 99.9)), vmin * 1.01)
    norm = colors.LogNorm(vmin=vmin, vmax=vmax)
    cmap = cm.get_cmap("turbo")

    fig, ax = ox.plot_graph(
        graph,
        node_size=0,
        edge_color="gray",
        edge_linewidth=0.4,
        bgcolor="white",
        show=False,
        close=False,
        figsize=(14, 14),
    )

    max_share = max(shares.values())
    for edge_key, share in sorted(shares.items(), key=lambda x: x[1]):
        geom = edges.loc[edge_key].geometry
        lines = [geom] if geom.geom_type == "LineString" else list(geom.geoms)
        for line in lines:
            ax.plot(
                *line.xy,
                color=cmap(norm(share)),
                linewidth=1.2 + 2.0 * np.sqrt(share / max_share),
                alpha=0.9,
            )

    cbar = fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax, fraction=0.03, pad=0.01)
    cbar.set_label("Anteil Gesamtgewicht (Kante / Summe aller Count)")

    ax.set_title("Nextbike-Heatmap auf Mannheim-Graph (OSRM, Anteil Gesamtgewicht)")
    fig.savefig(OUT_FILE, dpi=300, bbox_inches="tight")
    print("Bild gespeichert als:", OUT_FILE)


if __name__ == "__main__":
    main()
