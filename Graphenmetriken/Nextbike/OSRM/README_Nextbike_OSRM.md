### &#x20;NEXTBIKE - OSRM





Berechnet Graphmetriken (Degree, generalisierter Degree,

Betweenness-Zentralitaet) für das Mannheimer Radwegenetz auf

Basis von Nextbike-Ausleihen, deren Fahrtrouten mit OSRM

berechnet wurden.



Skript 1: "OSRM Grapherzeugen.py"

Skript 2: NetworkxGraphmetrikenBerechnen.py



#### DATEIEN UND IHRE ROLLE IM SKRIPT-PROZESS





mannheim\_bike.graphml

&#x20;   Radwegenetz Mannheim als OSMnx-Graph (Cache). Wird von

&#x20;   beiden Skripten geladen; falls nicht vorhanden, erzeugt

&#x20;   Skript 1 ihn neu per OSM-Download.



df\_nextbike\_merged\_mit\_routenOSRM.csv.xz

&#x20;   EINGABE für Skript 1. Nextbike-Ausleihen inkl. bereits

&#x20;   berechneter OSRM-Route je Fahrt (Spalte "route\_als\_liste",

&#x20;   Liste von Koordinaten). Das eigentliche OSRM-Routing selbst

&#x20;   ist nicht Teil dieses Ordners - die Datei enthält die

&#x20;   Routen bereits fertig berechnet.

&#x20;   **!** Für den Upload musste die .csv Datei stark komprimiert werden und muss erstmal entpackt werden und dann im selben Ordner wie die Python Skripte liegen



"OSRM Grapherzeugen.py"

&#x20;   SKRIPT 1. Matched Routen auf Graph-Kanten, erzeugt Heatmap,

&#x20;   baut Adjazenzmatrix. Details siehe unten.



OSRMedge\_matching\_cache.csv

&#x20;   ZWISCHENERGEBNIS von Skript 1. Zuordnung jeder Route zu

&#x20;   Graph-Kanten (Spalten u, v, k, route\_id). Dient als Cache:

&#x20;   existiert die Datei bereits, wird das Matching beim nächsten

&#x20;   Lauf übersprungen.



adjacency\_matrix.npz

&#x20;   ERGEBNIS von Skript 1 (sparse Adjazenzmatrix des auf

&#x20;   genutzte Kanten reduzierten Graphen). EINGABE für Skript 2.



OSRMnode\_index.csv

&#x20;   ERGEBNIS von Skript 1 (Zuordnung Matrix-Index <-> OSM-Knoten-ID).

&#x20;   EINGABE für Skript 2.



OSRMadjacency\_matrix\_dense.csv

&#x20;   Optionales ERGEBNIS von Skript 1 (dense Variante der

&#x20;   Adjazenzmatrix, nur falls Graph <= 5000 Knoten). Wird von

&#x20;   keinem weiteren Skript eingelesen, könnte auch zur weiteren Analyse verwendet werden.



NetworkxGraphmetrikenBerechnen.py

&#x20;   SKRIPT 2. Berechnet aus Adjazenzmatrix + Knotenindex die

&#x20;   Graphmetriken je Knoten. Details siehe unten.



nodes\_OSRMwithmetrics.csv

&#x20;   ENDERGEBNIS von Skript 2. Knotenliste mit Koordinaten,

&#x20;   Degree, generalisiertem Degree und Betweenness-Zentralität.





#### ABLAUF DER SKRIPTE



1\. OSRM Grapherzeugen.py

&#x20;  - Lädt df\_nextbike\_merged\_mit\_routenOSRM.csv und

&#x20;    mannheim\_bike.graphml.

&#x20;  - Matched für jede Route die Streckenabschnitte (Mittelpunkt

&#x20;    zwischen je zwei Koordinaten) per

&#x20;    osmnx.distance.nearest\_edges auf die nächstgelegene

&#x20;    Graph-Kante.

&#x20;  - Cached das Matching in OSRMedge\_matching\_cache.csv.

&#x20;  - Erzeugt eine Heatmap-Grafik der genutzten Kanten

&#x20;    (mannheim\_nextbike\_heatmapOSMR.png).

&#x20;  - Entfernt alle nie genutzten Kanten und dadurch isolierte

&#x20;    Knoten aus dem Graphen.

&#x20;  - Baut aus den verbleibenden Kanten eine sparse

&#x20;    Adjazenzmatrix -> adjacency\_matrix.npz + OSRMnode\_index.csv

&#x20;    (optional zusätzlich OSRMadjacency\_matrix\_dense.csv).



2\. NetworkxGraphmetrikenBerechnen.py

&#x20;  - Lädt adjacency\_matrix.npz und OSRMnode\_index.csv aus

&#x20;    Schritt 1.

&#x20;  - Rekonstruiert daraus einen networkx-Subgraphen (Selfloops

&#x20;    und isolierte Knoten werden entfernt).

&#x20;  - Berechnet je Knoten: Degree, generalisierten Degree

&#x20;    (networkx.generalized\_degree) und Betweenness-Zentralitaet

&#x20;    (networkx.betweenness\_centrality, mit endpoints=True).

&#x20;  - Schreibt das Ergebnis nach nodes\_OSRMwithmetrics.csv.



Ab hier dann Aufbereitung in eigenständigen Jupyter Notebooks (Top 5 Werte extrahieren und in Folium Karten einfügen)





#### HINWEISE



\- Identisches Pendant für Graphhopper-Routing liegt unter

&#x20; ../Graphhopper. Einziger inhaltlicher Unterschied: Quelle der

&#x20; Routengeometrien (OSRM vs. Graphhopper).

\- df\_nextbike\_merged\_mit\_routenOSRM.csv.xz muss erstmal entpackt werden, damit die Datei als Eingabe nutzbar ist!



