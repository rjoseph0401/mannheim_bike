import osmnx as ox
import matplotlib.pyplot as plt
import pandas as pd
import ast
import numpy as np
from pathlib import Path
from matplotlib import cm, colors
import scipy.sparse as sp
import networkx as nx
import json


# Matrix laden
adj_matrix = sp.load_npz("adjacency_matrix.npz")
adj_matrix.data[adj_matrix.data>1]=1 #korrigiere Einträge größer als 1 auf 1 runter für eine richtige Adjeszenzmatrix (Werte größer 1 kommen aus sparse transformation)

# Node-Index laden (um OSM-IDs zuzuordnen)
node_index = pd.read_csv("OSRMnode_index.csv")



G_full = ox.load_graphml("mannheim_bike.graphml")
G_full_undirected = ox.convert.to_undirected(G_full)
# Nur verbleibende Knoten aus dem gefilterten Graphen
nodes = node_index["osm_id"].tolist()
G_sub = G_full_undirected.subgraph(nodes).copy()                   #filtere den gesamtgraph auf den untergraph mit den Knoten, an denen eine Kante liegt


G_sub_multi_undirected = nx.to_undirected(G_sub)
G_sub_undirected = nx.Graph(G_sub_multi_undirected)

# 1. Remove self-loops
G_sub_undirected.remove_edges_from(nx.selfloop_edges(G_sub_undirected))

# 2. Remove isolated nodes (degree 0 — caused by subgraph boundary cut)
isolated = list(nx.isolates(G_sub_undirected))
print(f"Removing {len(isolated)} isolated nodes: {isolated}")
G_sub_undirected.remove_nodes_from(isolated)

# 3. Sync node_index to only keep nodes still in the graph
node_index = node_index[node_index["osm_id"].isin(G_sub_undirected.nodes())].reset_index(drop=True)


# Plot
fig, ax = ox.plot_graph(
    G_sub_multi_undirected,
    node_size=2,
    node_color="limegreen",
    edge_color="steelblue",
    edge_linewidth=0.5,
    bgcolor="white",
    show=False,
    close=False,
    figsize=(14, 14)
)
ax.set_title("Gefilterter Graph (nur Knoten mit Routen)")
fig.savefig("graph_filtered.png", dpi=300, bbox_inches="tight")
plt.show()

#Calculate the Degree of the Nodes via generalized Degree function from Networkx package
print("Degree")
Degree = nx.generalized_degree(G_sub_undirected) #no k specified => all nodes are calculated
#print(Degree)
print("centrality")
Betweenness_centrality = nx.betweenness_centrality(G_sub_undirected,endpoints=True)
#print(Betweenness_centrality)

#update the node List with the new data and Koordinates of the nodes
node_index.insert(2,"lon",None)
node_index.insert(3,"lat",None)
node_index = node_index.copy()
print(node_index)

node_index["Degree"] = None
node_index["generalized Degree"] = None
node_index["betweenness"] = None
for osm_id in node_index["osm_id"]:
    node_index.loc[node_index["osm_id"]==osm_id,"lon"] = G_sub_undirected.nodes[osm_id]["x"]
    node_index.loc[node_index["osm_id"]==osm_id,"lat"] = G_sub_undirected.nodes[osm_id]["y"]
    node_index.loc[node_index["osm_id"]==osm_id,"Degree"] = sum(Degree[osm_id].values())
    node_index.loc[node_index["osm_id"]==osm_id,"generalized Degree"] = [Degree.get(osm_id)]
    node_index.loc[node_index["osm_id"]==osm_id,"betweenness"] = Betweenness_centrality.get(osm_id)



print(node_index)
print("checks")
print(node_index[node_index["osm_id"]==113991681])
print(Degree.get(113991681)) #generalized Degree von 0 heißt, dass der Nachbar am Rand liegt und nur eine Verbindung in den Graphen hat, nicht dass der Knoten Isoliert ist!


print(node_index)
node_index.to_csv("nodes_OSRMwithmetrics.csv")
