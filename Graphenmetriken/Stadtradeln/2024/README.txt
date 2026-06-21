================================================================
 STADTRADELN 2024
================================================================

Berechnet Graphmetriken (Degree, generalisierter Degree,
Betweenness-Zentralität) für das Mannheimer Radwegenetz auf
Basis der Stadtradeln-Zähldaten 2024. Gleiches dreistufiges
Vorgehen wie ../2022 und ../2023, mit zwei Abweichungen:
anderes Spaltenschema der Rohdaten und zusätzliche, logarithmisch
skalierte Heatmap-Visualisierungen.

----------------------------------------------------------------
DATEIEN UND IHRE ROLLE IM SKRIPT-PROZESS
----------------------------------------------------------------

stadtradeln_2024.xlsx  (FEHLT in diesem Ordner)
    EINGABE für Skript 1. Beachte unterschiedliches Koordinaten-Format.

stadtradeln_osrm_heatmap.py
    SKRIPT 1. Führt das OSRM-Routing durch und erzeugt die
    Routengeometrien + zwei Heatmap-Visualisierungen. Details
    siehe unten. 
stadtradeln2024_osrm_routes.geojson
    ERGEBNIS von Skript 1: alle gerouteten
    Strecken als GeoJSON. EINGABE für Skript 2.

mannheim_bike.graphml
    Radwegenetz Mannheim (kleiner Graph, exakte Stadtgrenze) -
    Cache für Skript 2 und 3.

StadtradelnGrapherzeugen.py
    SKRIPT 2. Matched Routen auf Graph-Kanten, clippt auf
    Mannheimer Stadtgebiet, baut Adjazenzmatrix. Details siehe
    unten.

edge_matching_cache_Stadtradeln2024.csv
    ZWISCHENERGEBNIS von Skript 2. Zuordnung jeder Route zu
    Graph-Kanten (Spalten u, v, k, route_id). Cache für
    wiederholte Läufe.

adjacency_matrixStadtradeln2024.npz
    ERGEBNIS von Skript 2 (sparse Adjazenzmatrix des auf
    Mannheim geclippten, genutzten Graphen). EINGABE für
    Skript 3.

node_indexStadtradeln2024.csv
    ERGEBNIS von Skript 2 (Zuordnung Matrix-Index <-> OSM-Knoten-ID).
    EINGABE für Skript 3.

NetworkxGraphmetrikenBerechnenStadtradeln2024.py
    SKRIPT 3. Berechnet aus Adjazenzmatrix + Knotenindex die
    Graphmetriken je Knoten. Details siehe unten.

Stadtradeln2024nodes_OSRMwithmetrics.csv
    ENDERGEBNIS von Skript 3. Knotenliste mit Koordinaten,
    Degree, generalisiertem Degree und Betweenness-Zentralität.

----------------------------------------------------------------
ABLAUF DER SKRIPTE
----------------------------------------------------------------

1. stadtradeln_osrm_heatmap.py
   - Liest stadtradeln_2024.xlsx (Spalten x_start/y_start/x_end/
     y_end in EPSG:25832, transformiert nach WGS84; Spalte
     number_of_matched_trips), fasst identische Start-Ziel-Paare
     zusammen.
   - Schickt jede einzigartige Verbindung parallel an den
     öffentlichen OSRM-Dienst (routing.openstreetmap.de).
   - Cached die Antworten in cache/stadtradeln_osrm_routes.json.
   - Speichert das Ergebnis als GeoJSON
     (stadtradeln2024_osrm_routes.geojson) und als interaktive
     Folium-Karte.
   - Erzeugt zusätzlich eine STATISCHE, logarithmisch skalierte
     Heatmap-Grafik (stadtradeln_osrm_heatmap.png) auf Basis des
     kompletten Mannheimer Straßennetzes.

2. StadtradelnGrapherzeugen.py
   - Liest stadtradeln2024_osrm_routes.geojson aus Schritt 1.
   - Nutzt einen großen Graphen mit 15 km Puffer um Mannheim
     (Matching) sowie den kleinen, exakten Mannheim-Graphen
     (finales Clipping).
   - Matched jede Route per Mittelpunkt-Verfahren auf die
     nächste Kante (Cache
     edge_matching_cache_Stadtradeln2024.csv).
   - Erzeugt vor dem Clipping zusätzlich eine ungeclippte,
     logarithmisch skalierte Heatmap-Grafik
     (stadtradeln2024_heatmap_log_ungeclippt.png).
   - Entfernt ungenutzte Kanten sowie Knoten außerhalb der
     Mannheimer Stadtgrenze.
   - Baut die Adjazenzmatrix ->
     adjacency_matrixStadtradeln2024.npz +
     node_indexStadtradeln2024.csv.

3. NetworkxGraphmetrikenBerechnenStadtradeln2024.py
   - Lädt adjacency_matrixStadtradeln2024.npz und
     node_indexStadtradeln2024.csv aus Schritt 2.
   - Rekonstruiert daraus einen networkx-Subgraphen (Selfloops
     und isolierte Knoten werden entfernt).
   - Berechnet je Knoten: Degree, generalisierten Degree und
     Betweenness-Zentralität.
   - Schreibt das Ergebnis nach
     Stadtradeln2024nodes_OSRMwithmetrics.csv.

----------------------------------------------------------------
HINWEISE
----------------------------------------------------------------

- Gleiches dreistufiges Grundprinzip wie ../2022 und ../2023,
  jedoch mit abweichendem Spaltenschema der Rohdaten und
  zusätzlichen Log-Heatmap-Plots in Skript 1 und 2.

