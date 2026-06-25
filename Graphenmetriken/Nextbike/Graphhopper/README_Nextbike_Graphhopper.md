

### &#x20;**NEXTBIKE - GRAPHHOPPER**



Berechnet Graphmetriken (Degree, generalisierter Degree,

Betweenness-Zentralität) für das Mannheimer Radwegenetz auf

Basis von Nextbike-Ausleihen, deren Fahrtrouten mit Graphhopper

berechnet wurden. Funktional identisch zu ../OSRM, nur mit

Graphhopper als Routing-Quelle.



Skript 1: Graphhopper\_Graphmetriken.py

Skript 2: Graphhopper\_NetworkxGraphmetrikenBerechnen.py



#### DATEIEN UND IHRE ROLLE IM SKRIPT-PROZESS





mannheim\_bike.graphml

&#x20;   Radwegenetz Mannheim als OSMnx-Graph (Cache). Wird von

&#x20;   beiden Skripten geladen.



Df\_NextbikemitGraphhopperroutes.csv.zip

&#x20;   EINGABE für Skript 1. Nextbike-Ausleihen inkl. bereits

&#x20;   berechneter Graphhopper-Route je Fahrt (Spalte

&#x20;   "route\_als\_liste"). Das eigentliche Graphhopper-Routing

&#x20;   selbst ist nicht Teil dieses Ordners.

&#x20;   **!Das Dataframe wurde als zip Komprimiert, um upload zu ermöglichen.**



routes\_graphhopper.csv

&#x20;   Rohe Graphhopper-Koordinaten je Ausleihe, aggregiert nach

&#x20;   Start-/End-Station (Spalten GraphhopperKoordinaten,

&#x20;   StartStationID, EndStationID, count). 



Graphhopper\_Graphmetriken.py

&#x20;   SKRIPT 1. Matched Routen auf Graph-Kanten, erzeugt Heatmap,

&#x20;   baut Adjazenzmatrix. Details siehe unten.



edge\_matching\_cacheGraphhopper.csv

&#x20;   ZWISCHENERGEBNIS von Skript 1. Zuordnung jeder Route zu

&#x20;   Graph-Kanten (Spalten u, v, k, route\_id). Cache für

&#x20;   wiederholte Läufe.



adjacency\_matrixGraphhopper.npz

&#x20;   ERGEBNIS von Skript 1 (sparse Adjazenzmatrix des auf

&#x20;   genutzte Kanten reduzierten Graphen). EINGABE für Skript 2.



node\_indexGraphhopper.csv

&#x20;   ERGEBNIS von Skript 1 (Zuordnung Matrix-Index <-> OSM-Knoten-ID).

&#x20;   EINGABE für Skript 2.



Graphhopper\_NetworkxGraphmetrikenBerechnen.py

&#x20;   SKRIPT 2. Berechnet aus Adjazenzmatrix + Knotenindex die

&#x20;   Graphmetriken je Knoten. Details siehe unten.



nodes\_Graphhopperwithmetrics.csv

&#x20;   ENDERGEBNIS von Skript 2. Knotenliste mit Koordinaten,

&#x20;   Degree, generalisiertem Degree und Betweenness-Zentralität.





#### ABLAUF DER SKRIPTE



1\. Graphhopper\_Graphmetriken.py

&#x20;  - Lädt Df\_NextbikemitGraphhopperroutes.csv und

&#x20;    mannheim\_bike.graphml.

&#x20;  - Matched für jede Route die Streckenabschnitte (Mittelpunkt

&#x20;    zwischen je zwei Koordinaten) per

&#x20;    osmnx.distance.nearest\_edges auf die nächstliegende

&#x20;    Graph-Kante.

&#x20;  - Cached das Matching in edge\_matching\_cacheGraphhopper.csv.

&#x20;  - Erzeugt eine Heatmap-Grafik der genutzten Kanten

&#x20;    (mannheim\_nextbike\_heatmapGraphhopper.png).

&#x20;  - Entfernt alle nie genutzten Kanten und dadurch isolierte

&#x20;    Knoten aus dem Graphen.

&#x20;  - Baut aus den verbleibenden Kanten eine sparse

&#x20;    Adjazenzmatrix -> adjacency\_matrixGraphhopper.npz +

&#x20;    node\_indexGraphhopper.csv (optional zusätzlich

&#x20;    adjacency\_matrix\_denseGraphhopper.csv, falls Graph

&#x20;    <= 5000 Knoten).



2\. Graphhopper\_NetworkxGraphmetrikenBerechnen.py

&#x20;  - Lädt adjacency\_matrixGraphhopper.npz und

&#x20;    node\_indexGraphhopper.csv aus Schritt 1.

&#x20;  - Rekonstruiert daraus einen networkx-Subgraphen (Selfloops

&#x20;    und isolierte Knoten werden entfernt).

&#x20;  - Berechnet je Knoten: Degree, generalisierten Degree und

&#x20;    Betweenness-Zentralitaet.

&#x20;  - Schreibt das Ergebnis nach nodes\_Graphhopperwithmetrics.csv.



Ab hier dann Aufbereitung in eigenständigen Jupyter Notebooks (Top 5 Werte extrahieren und in Folium Karten einfügen)



###### HINWEISE



\- Identisches Pendant für OSRM-Routing liegt unter ../OSRM.

\- Df\_NextbikemitGraphhopperroutes.csv.zip muss erst entpackt werden und im selben Ordner liegen wie die anderen Python Skripte!





