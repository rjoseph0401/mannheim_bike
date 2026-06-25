

### &#x20;STADTRADELN 2024





Berechnet Graphmetriken (Degree, generalisierter Degree,

Betweenness-Zentralität) für das Mannheimer Radwegenetz auf

Basis der Stadtradeln-Zähldaten 2024. Gleiches dreistufiges

Vorgehen wie ../2022 und ../2023, mit zwei Abweichungen:

anderes Spaltenschema der Rohdaten und zusätzliche, logarithmisch

skalierte Heatmap-Visualisierungen.



Skript 1: stadtradeln\_osrm\_heatmap.py

Skript 2: StadtradelnGrapherzeugen.py

Skript 3: NetworkxGraphmetrikenBerechnenStadtradeln2024.py





#### DATEIEN UND IHRE ROLLE IM SKRIPT-PROZESS





stadtradeln\_2024.xlsx  

&#x20;   EINGABE für Skript 1. Beachte unterschiedliches Koordinaten-Format.



stadtradeln\_osrm\_heatmap.py

&#x20;   SKRIPT 1. Führt das OSRM-Routing durch und erzeugt die

&#x20;   Routengeometrien + zwei Heatmap-Visualisierungen. Details

&#x20;   siehe unten. 



stadtradeln2024\_osrm\_routes.geojson

&#x20;   ERGEBNIS von Skript 1: alle gerouteten

&#x20;   Strecken als GeoJSON. EINGABE für Skript 2.

**!** Wegen Dateigröße in den zip Ordner big-files ausgelagert, dieser muss erstmal extrahiert werden





mannheim\_bike.graphml

&#x20;   Radwegenetz Mannheim (kleiner Graph, exakte Stadtgrenze) -

&#x20;   Cache für Skript 2 und 3.

**!** Wegen Dateigröße in den zip Ordner big-files ausgelagert, dieser muss erstmal extrahiert werden





StadtradelnGrapherzeugen.py

&#x20;   SKRIPT 2. Matched Routen auf Graph-Kanten, clippt auf

&#x20;   Mannheimer Stadtgebiet, baut Adjazenzmatrix. Details siehe

&#x20;   unten.



edge\_matching\_cache\_Stadtradeln2024.csv

&#x20;   ZWISCHENERGEBNIS von Skript 2. Zuordnung jeder Route zu

&#x20;   Graph-Kanten (Spalten u, v, k, route\_id). Cache für

&#x20;   wiederholte Läufe.



adjacency\_matrixStadtradeln2024.npz

&#x20;   ERGEBNIS von Skript 2 (sparse Adjazenzmatrix des auf

&#x20;   Mannheim geclippten, genutzten Graphen). EINGABE für

&#x20;   Skript 3.



node\_indexStadtradeln2024.csv

&#x20;   ERGEBNIS von Skript 2 (Zuordnung Matrix-Index <-> OSM-Knoten-ID).

&#x20;   EINGABE für Skript 3.



NetworkxGraphmetrikenBerechnenStadtradeln2024.py

&#x20;   SKRIPT 3. Berechnet aus Adjazenzmatrix + Knotenindex die

&#x20;   Graphmetriken je Knoten. Details siehe unten.



Stadtradeln2024nodes\_OSRMwithmetrics.csv

&#x20;   ENDERGEBNIS von Skript 3. Knotenliste mit Koordinaten,

&#x20;   Degree, generalisiertem Degree und Betweenness-Zentralität.





#### ABLAUF DER SKRIPTE





1\. stadtradeln\_osrm\_heatmap.py

&#x20;  - Liest stadtradeln\_2024.xlsx (Spalten x\_start/y\_start/x\_end/

&#x20;    y\_end in EPSG:25832, transformiert nach WGS84; Spalte

&#x20;    number\_of\_matched\_trips), fasst identische Start-Ziel-Paare

&#x20;    zusammen.

&#x20;  - Schickt jede einzigartige Verbindung parallel an den

&#x20;    öffentlichen OSRM-Dienst (routing.openstreetmap.de).

&#x20;  - Cached die Antworten in cache/stadtradeln\_osrm\_routes.json.

&#x20;  - Speichert das Ergebnis als GeoJSON

&#x20;    (stadtradeln2024\_osrm\_routes.geojson) und als interaktive

&#x20;    Folium-Karte.

&#x20;  - Erzeugt zusätzlich eine STATISCHE, logarithmisch skalierte

&#x20;    Heatmap-Grafik (stadtradeln\_osrm\_heatmap.png) auf Basis des

&#x20;    kompletten Mannheimer Straßennetzes.



2\. StadtradelnGrapherzeugen.py

&#x20;  - Liest stadtradeln2024\_osrm\_routes.geojson aus Schritt 1.

&#x20;  - Nutzt einen großen Graphen mit 15 km Puffer um Mannheim

&#x20;    (Matching) sowie den kleinen, exakten Mannheim-Graphen

&#x20;    (finales Clipping).

&#x20;  - Matched jede Route per Mittelpunkt-Verfahren auf die

&#x20;    nächste Kante (Cache

&#x20;    edge\_matching\_cache\_Stadtradeln2024.csv).

&#x20;  - Erzeugt vor dem Clipping zusätzlich eine ungeclippte,

&#x20;    logarithmisch skalierte Heatmap-Grafik

&#x20;    (stadtradeln2024\_heatmap\_log\_ungeclippt.png).

&#x20;  - Entfernt ungenutzte Kanten sowie Knoten außerhalb der

&#x20;    Mannheimer Stadtgrenze.

&#x20;  - Baut die Adjazenzmatrix ->

&#x20;    adjacency\_matrixStadtradeln2024.npz +

&#x20;    node\_indexStadtradeln2024.csv.



3\. NetworkxGraphmetrikenBerechnenStadtradeln2024.py

&#x20;  - Lädt adjacency\_matrixStadtradeln2024.npz und

&#x20;    node\_indexStadtradeln2024.csv aus Schritt 2.

&#x20;  - Rekonstruiert daraus einen networkx-Subgraphen (Selfloops

&#x20;    und isolierte Knoten werden entfernt).

&#x20;  - Berechnet je Knoten: Degree, generalisierten Degree und

&#x20;    Betweenness-Zentralität.

&#x20;  - Schreibt das Ergebnis nach

&#x20;    Stadtradeln2024nodes\_OSRMwithmetrics.csv.



Ab hier dann Aufbereitung in eigenständigen Jupyter Notebooks (Top 5 Werte extrahieren und in Folium Karten einfügen)





###### HINWEISE



\- Gleiches dreistufiges Grundprinzip wie ../2022 und ../2023,

&#x20; jedoch mit abweichendem Spaltenschema der Rohdaten und

&#x20; zusätzlichen Log-Heatmap-Plots in Skript 1 und 2.

\- Der zip-Ordner "**Big files**" enthält Dateien die zu groß waren für den Upload nach git-Hub, dieser muss zuerst entpackt werden







