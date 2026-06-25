### &#x20;STADTRADELN 2022





Berechnet Graphmetriken (Degree, generalisierter Degree,

Betweenness-Zentralität) für das Mannheimer Radwegenetz auf

Basis der Stadtradeln-Zähldaten 2022. Im Gegensatz zu den

Nextbike-Ordnern wird das Routing (OSRM) hier selbst durch-

geführt, daher gibt es einen zusätzlichen, vorgelagerten

Schritt (3 Skripte statt 2).



Skript 1: stadtradeln2022\_osrm\_heatmap.py

Skript 2: Stadtradeln2022Grapherzeugen.py

Skript 3: NetworkxGraphmetrikenBerechnenStadtradeln2022.py





#### DATEIEN UND IHRE ROLLE IM SKRIPT-PROZESS





stadtradeln\_2022.xlsx

&#x20;   EINGABE für Skript 1. Rohdaten der Stadtradeln-Zählungen:

&#x20;   Start-/Endkoordinaten und Fahrtenanzahl je Verbindung

&#x20;   (Spalten count, geometry, start\_lon, start\_lat, end\_lon,

&#x20;   end\_lat).



stadtradeln2022\_osrm\_heatmap.py

&#x20;   SKRIPT 1. Führt das OSRM-Routing durch und erzeugt die

&#x20;   Routengeometrien. Details siehe unten.



stadtradeln2022\_osrm\_routes.geojson

&#x20;   ERGEBNIS von Skript 1: alle gerouteten Strecken als GeoJSON

&#x20;   (Geometrie + Anzahl Fahrten je Strecke). EINGABE für

&#x20;   Skript 2.

**!** Wegen Dateigröße in den zip Ordner big-files ausgelagert, dieser muss erstmal extrahiert werden



mannheim\_bike.graphml

&#x20;   Radwegenetz Mannheim (kleiner Graph, exakte Stadtgrenze) -

&#x20;   Cache für Skript 2 und 3. Ein zusätzlicher "großer" Graph

&#x20;   mit 15 km Puffer (mannheim\_bike\_large.graphml) wird von

&#x20;   Skript 2 bei Bedarf automatisch neu erzeugt, falls nicht

&#x20;   vorhanden.

**!** Wegen Dateigröße in den zip Ordner big-files ausgelagert, dieser muss erstmal extrahiert werden





Stadtradeln2022Grapherzeugen.py

&#x20;   SKRIPT 2. Matched Routen auf Graph-Kanten, clippt auf

&#x20;   Mannheimer Stadtgebiet, baut Adjazenzmatrix. Details siehe

&#x20;   unten.



edge\_matching\_cache\_Stadtradeln2022.csv

&#x20;   ZWISCHENERGEBNIS von Skript 2. Zuordnung jeder Route zu

&#x20;   Graph-Kanten (Spalten u, v, k, route\_id). Cache für

&#x20;   wiederholte Läufe.



adjacency\_matrixStadtradeln2022.npz

&#x20;   ERGEBNIS von Skript 2 (sparse Adjazenzmatrix des auf

&#x20;   Mannheim geclippten, genutzten Graphen). EINGABE für

&#x20;   Skript 3.



node\_indexStadtradeln2022.csv

&#x20;   ERGEBNIS von Skript 2 (Zuordnung Matrix-Index <-> OSM-Knoten-ID).

&#x20;   EINGABE für Skript 3.



NetworkxGraphmetrikenBerechnenStadtradeln2022.py

&#x20;   SKRIPT 3. Berechnet aus Adjazenzmatrix + Knotenindex die

&#x20;   Graphmetriken je Knoten. Details siehe unten.



Stadtradeln2022nodes\_OSRMwithmetrics.csv

&#x20;   ENDERGEBNIS von Skript 3. Knotenliste mit Koordinaten,

&#x20;   Degree, generalisiertem Degree und Betweenness-Zentralität.





#### ABLAUF DER SKRIPTE





1\. stadtradeln2022\_osrm\_heatmap.py

&#x20;  - Liest stadtradeln\_2022.xlsx, fasst identische Start-Ziel-

&#x20;    Paare zusammen.

&#x20;  - Schickt jede einzigartige Verbindung parallel an den

&#x20;    öffentlichen OSRM-Dienst (routing.openstreetmap.de).

&#x20;  - Cached die Antworten in cache/stadtradeln2022\_osrm\_routes.json

&#x20;    (verhindert erneute Abfragen bei wiederholtem Lauf).

&#x20;  - Speichert das Ergebnis als GeoJSON

&#x20;    (stadtradeln2022\_osrm\_routes.geojson) und zusätzlich als

&#x20;    interaktive Folium-Karte (stadtradeln2022\_osrm\_heatmap\_new.html).



2\. Stadtradeln2022Grapherzeugen.py

&#x20;  - Liest stadtradeln2022\_osrm\_routes.geojson aus Schritt 1.

&#x20;  - Nutzt zwei Graphen: einen großen Graphen mit 15 km Puffer

&#x20;    um Mannheim (für das Matching auch außerhalb liegender

&#x20;    Routenabschnitte) und den kleinen, exakten Mannheim-Graphen

&#x20;    (zum finalen Clipping).

&#x20;  - Matched jede Route per Mittelpunkt-Verfahren auf die

&#x20;    nächste Kante des großen Graphen

&#x20;    (Cache edge\_matching\_cache\_Stadtradeln2022.csv).

&#x20;  - Erzeugt eine Heatmap (mannheim\_Stadtradeln2022\_heatmapOSMR.png).

&#x20;  - Entfernt ungenutzte Kanten sowie alle Knoten außerhalb der

&#x20;    Mannheimer Stadtgrenze (per ox.geocode\_to\_gdf).

&#x20;  - Baut aus dem verbleibenden Graphen die Adjazenzmatrix ->

&#x20;    adjacency\_matrixStadtradeln2022.npz +

&#x20;    node\_indexStadtradeln2022.csv.



3\. NetworkxGraphmetrikenBerechnenStadtradeln2022.py

&#x20;  - Lädt adjacency\_matrixStadtradeln2022.npz und

&#x20;    node\_indexStadtradeln2022.csv aus Schritt 2.

&#x20;  - Rekonstruiert daraus einen networkx-Subgraphen (Selfloops

&#x20;    und isolierte Knoten werden entfernt).

&#x20;  - Berechnet je Knoten: Degree, generalisierten Degree und

&#x20;    Betweenness-Zentralität.

&#x20;  - Schreibt das Ergebnis nach

&#x20;    Stadtradeln2022nodes\_OSRMwithmetrics.csv.



Ab hier dann Aufbereitung in eigenständigen Jupyter Notebooks (Top 5 Werte extrahieren und in Folium Karten einfügen)



#### HINWEISE



\- Identischer Aufbau wie ../2023; ../2024 enthält zusätzlich

&#x20; eine erweiterte, logarithmisch skalierte Heatmap-Visualisierung

&#x20; und hat ein leicht anderes Rohdaten-Schema.

\- Der zip-Ordner "**Big files**" enthält Dateien die zu groß waren für den Upload nach git-Hub, dieser muss zuerst entpackt werden



