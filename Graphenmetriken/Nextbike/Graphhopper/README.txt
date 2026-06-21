================================================================
 NEXTBIKE - GRAPHHOPPER
================================================================

Berechnet Graphmetriken (Degree, generalisierter Degree,
Betweenness-Zentralität) für das Mannheimer Radwegenetz auf
Basis von Nextbike-Ausleihen, deren Fahrtrouten mit Graphhopper
berechnet wurden. Funktional identisch zu ../OSRM, nur mit
Graphhopper als Routing-Quelle.

----------------------------------------------------------------
DATEIEN UND IHRE ROLLE IM SKRIPT-PROZESS
----------------------------------------------------------------

mannheim_bike.graphml
    Radwegenetz Mannheim als OSMnx-Graph (Cache). Wird von
    beiden Skripten geladen.

Df_NextbikemitGraphhopperroutes.csv
    EINGABE für Skript 1. Nextbike-Ausleihen inkl. bereits
    berechneter Graphhopper-Route je Fahrt (Spalte
    "route_als_liste"). Das eigentliche Graphhopper-Routing
    selbst ist nicht Teil dieses Ordners.

routes_graphhopper.csv
    Rohe Graphhopper-Koordinaten je Ausleihe, aggregiert nach
    Start-/End-Station (Spalten GraphhopperKoordinaten,
    StartStationID, EndStationID, count). 

Graphhopper_Graphmetriken.py
    SKRIPT 1. Matched Routen auf Graph-Kanten, erzeugt Heatmap,
    baut Adjazenzmatrix. Details siehe unten.

edge_matching_cacheGraphhopper.csv
    ZWISCHENERGEBNIS von Skript 1. Zuordnung jeder Route zu
    Graph-Kanten (Spalten u, v, k, route_id). Cache für
    wiederholte Läufe.

adjacency_matrixGraphhopper.npz
    ERGEBNIS von Skript 1 (sparse Adjazenzmatrix des auf
    genutzte Kanten reduzierten Graphen). EINGABE für Skript 2.

node_indexGraphhopper.csv
    ERGEBNIS von Skript 1 (Zuordnung Matrix-Index <-> OSM-Knoten-ID).
    EINGABE für Skript 2.

Graphhopper_NetworkxGraphmetrikenBerechnen.py
    SKRIPT 2. Berechnet aus Adjazenzmatrix + Knotenindex die
    Graphmetriken je Knoten. Details siehe unten.

nodes_Graphhopperwithmetrics.csv
    ENDERGEBNIS von Skript 2. Knotenliste mit Koordinaten,
    Degree, generalisiertem Degree und Betweenness-Zentralität.

----------------------------------------------------------------
ABLAUF DER SKRIPTE
----------------------------------------------------------------

1. Graphhopper_Graphmetriken.py
   - Lädt Df_NextbikemitGraphhopperroutes.csv und
     mannheim_bike.graphml.
   - Matched für jede Route die Streckenabschnitte (Mittelpunkt
     zwischen je zwei Koordinaten) per
     osmnx.distance.nearest_edges auf die nächstliegende
     Graph-Kante.
   - Cached das Matching in edge_matching_cacheGraphhopper.csv.
   - Erzeugt eine Heatmap-Grafik der genutzten Kanten
     (mannheim_nextbike_heatmapGraphhopper.png).
   - Entfernt alle nie genutzten Kanten und dadurch isolierte
     Knoten aus dem Graphen.
   - Baut aus den verbleibenden Kanten eine sparse
     Adjazenzmatrix -> adjacency_matrixGraphhopper.npz +
     node_indexGraphhopper.csv (optional zusätzlich
     adjacency_matrix_denseGraphhopper.csv, falls Graph
     <= 5000 Knoten).

2. Graphhopper_NetworkxGraphmetrikenBerechnen.py
   - Lädt adjacency_matrixGraphhopper.npz und
     node_indexGraphhopper.csv aus Schritt 1.
   - Rekonstruiert daraus einen networkx-Subgraphen (Selfloops
     und isolierte Knoten werden entfernt).
   - Berechnet je Knoten: Degree, generalisierten Degree und
     Betweenness-Zentralitaet.
   - Schreibt das Ergebnis nach nodes_Graphhopperwithmetrics.csv.

----------------------------------------------------------------
HINWEISE
----------------------------------------------------------------

- Identisches Pendant für OSRM-Routing liegt unter ../OSRM.

