from pathlib import Path
import osmnx as ox

BASE_DIR = Path(__file__).resolve().parent  # mannheim_bike/
GRAPH_FILE = BASE_DIR / "mannheim_bike.graphml"
OUTPUT_FILE = BASE_DIR / "mannheim_bike_hauptachsen.graphml"
KEEP_HIGHWAY = {"trunk", "primary", "secondary", "tertiary", "cycleway"}

G = ox.load_graphml(GRAPH_FILE)
edges = ox.graph_to_gdfs(G, nodes=False, edges=True)

def hw_matches(val):
    vals = val if isinstance(val, list) else [val]
    return any(str(v) in KEEP_HIGHWAY for v in vals)

keep = {idx for idx, hw in edges["highway"].items() if hw_matches(hw)}

nodes = {u for u, _, _ in keep} | {v for _, v, _ in keep}
H = G.subgraph(nodes).copy()
for u, v, k in list(H.edges(keys=True)):
    if (u, v, k) not in keep:
        H.remove_edge(u, v, k)
H.remove_nodes_from([n for n, d in H.degree() if d == 0])

ox.save_graphml(H, OUTPUT_FILE)
print(f"Kanten: {len(G.edges)} -> {len(H.edges)}")
print(f"Knoten: {len(G.nodes)} -> {len(H.nodes)}")
print("Gespeichert:", OUTPUT_FILE)

fig, ax = ox.plot_graph(
    H,
    bgcolor="white",
    node_size=8,
    node_color="steelblue",
    edge_color="#444444",
    edge_linewidth=0.8,
    show=True,
    close=False,
)
