================================================================
 NEXTBIKE - OSRM
================================================================
!
! Das Dataframe mit den Routen am Nextbike Dateaframe wurde stark komprimiert, damit es hochladbar ist, muss entpackt werden, damit nutzbar im Code
!


Berechnet Graphmetriken (Degree, generalisierter Degree,
Betweenness-Zentralitaet) für das Mannheimer Radwegenetz auf
Basis von Nextbike-Ausleihen, deren Fahrtrouten mit OSRM
berechnet wurden.

----------------------------------------------------------------
DATEIEN UND IHRE ROLLE IM SKRIPT-PROZESS
----------------------------------------------------------------

mannheim_bike.graphml
    Radwegenetz Mannheim als OSMnx-Graph (Cache). Wird von
    beiden Skripten geladen; falls nicht vorhanden, erzeugt
    Skript 1 ihn neu per OSM-Download.

df_nextbike_merged_mit_routenOSRM.csv.xz
    EINGABE für Skript 1. Nextbike-Ausleihen inkl. bereits
    berechneter OSRM-Route je Fahrt (Spalte "route_als_liste",
    Liste von Koordinaten). Das eigentliche OSRM-Routing selbst
    ist nicht Teil dieses Ordners - die Datei enthält die
    Routen bereits fertig berechnet. Das Dataframe ist als .xz komprimiert um upload auf das git zu ermöglichen. Es  muss entpackt werden!

"OSRM Grapherzeugen.py"
    SKRIPT 1. Matched Routen auf Graph-Kanten, erzeugt Heatmap,
    baut Adjazenzmatrix. Details siehe unten.

OSRMedge_matching_cache.csv
    ZWISCHENERGEBNIS von Skript 1. Zuordnung jeder Route zu
    Graph-Kanten (Spalten u, v, k, route_id). Dient als Cache:
    existiert die Datei bereits, wird das Matching beim nächsten
    Lauf übersprungen.

adjacency_matrix.npz
    ERGEBNIS von Skript 1 (sparse Adjazenzmatrix des auf
    genutzte Kanten reduzierten Graphen). EINGABE für Skript 2.

OSRMnode_index.csv
    ERGEBNIS von Skript 1 (Zuordnung Matrix-Index <-> OSM-Knoten-ID).
    EINGABE für Skript 2.

OSRMadjacency_matrix_dense.csv
    Optionales ERGEBNIS von Skript 1 (dense Variante der
    Adjazenzmatrix, nur falls Graph <= 5000 Knoten). Wird von
    keinem weiteren Skript eingelesen, dient nur der Kontrolle.

NetworkxGraphmetrikenBerechnen.py
    SKRIPT 2. Berechnet aus Adjazenzmatrix + Knotenindex die
    Graphmetriken je Knoten. Details siehe unten.

nodes_OSRMwithmetrics.csv
    ENDERGEBNIS von Skript 2. Knotenliste mit Koordinaten,
    Degree, generalisiertem Degree und Betweenness-Zentralität.

----------------------------------------------------------------
ABLAUF DER SKRIPTE
----------------------------------------------------------------

1. "OSRM Grapherzeugen.py"
   - Lädt df_nextbike_merged_mit_routenOSRM.csv und
     mannheim_bike.graphml.
   - Matched fuer jede Route die Streckenabschnitte (Mittelpunkt
     zwischen je zwei Koordinaten) per
     osmnx.distance.nearest_edges auf die naechstgelegene
     Graph-Kante.
   - Cached das Matching in OSRMedge_matching_cache.csv.
   - Erzeugt eine Heatmap-Grafik der genutzten Kanten
     (mannheim_nextbike_heatmapOSMR.png).
   - Entfernt alle nie genutzten Kanten und dadurch isolierte
     Knoten aus dem Graphen.
   - Baut aus den verbleibenden Kanten eine sparse
     Adjazenzmatrix -> adjacency_matrix.npz + OSRMnode_index.csv
     (optional zusaetzlich OSRMadjacency_matrix_dense.csv).

2. NetworkxGraphmetrikenBerechnen.py
   - Lädt adjacency_matrix.npz und OSRMnode_index.csv aus
     Schritt 1.
   - Rekonstruiert daraus einen networkx-Subgraphen (Selfloops
     und isolierte Knoten werden entfernt).
   - Berechnet je Knoten: Degree, generalisierten Degree
     (networkx.generalized_degree) und Betweenness-Zentralitaet
     (networkx.betweenness_centrality, mit endpoints=True).
   - Schreibt das Ergebnis nach nodes_OSRMwithmetrics.csv.

----------------------------------------------------------------
HINWEISE
----------------------------------------------------------------

- Identisches Pendant für Graphhopper-Routing liegt unter
  ../Graphhopper. Einziger inhaltlicher Unterschied: Quelle der
  Routengeometrien (OSRM vs. Graphhopper).
