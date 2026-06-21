================================================================
 STADTRADELN 2023
================================================================

Berechnet Graphmetriken (Degree, generalisierter Degree,
Betweenness-Zentralität) für das Mannheimer Radwegenetz auf
Basis der Stadtradeln-Zähldaten 2023. Aufbau und Funktionsweise
sind identisch zu ../2022 - einziger Unterschied sind Jahreszahl
in Datei-/Variablennamen und das zugrundeliegende Datenjahr.

----------------------------------------------------------------
DATEIEN UND IHRE ROLLE IM SKRIPT-PROZESS
----------------------------------------------------------------

stadtradeln_2023.xlsx
    EINGABE für Skript 1. Rohdaten der Stadtradeln-Zählungen:
    Start-/Endkoordinaten und Fahrtenanzahl je Verbindung
    (Spalten count, geometry, start_lon, start_lat, end_lon,
    end_lat).

stadtradeln2023_osrm_heatmap.py
    SKRIPT 1. Führt das OSRM-Routing durch und erzeugt die
    Routengeometrien. Details siehe unten.

stadtradeln2023_osrm_routes.geojson
    ERGEBNIS von Skript 1: alle gerouteten Strecken als GeoJSON
    (Geometrie + Anzahl Fahrten je Strecke). EINGABE für
    Skript 2.

mannheim_bike.graphml
    Radwegenetz Mannheim (kleiner Graph, exakte Stadtgrenze) -
    Cache für Skript 2 und 3. Ein zusätzlicher "großer" Graph
    mit 15 km Puffer (mannheim_bike_large.graphml) wird von
    Skript 2 bei Bedarf automatisch neu erzeugt, falls nicht
    vorhanden.

Stadtradeln2023Grapherzeugen.py
    SKRIPT 2. Matched Routen auf Graph-Kanten, clippt auf
    Mannheimer Stadtgebiet, baut Adjazenzmatrix. Details siehe
    unten.

edge_matching_cache_Stadtradeln2023.csv
    ZWISCHENERGEBNIS von Skript 2. Zuordnung jeder Route zu
    Graph-Kanten (Spalten u, v, k, route_id). Cache für
    wiederholte Läufe.

adjacency_matrixStadtradeln2023.npz
    ERGEBNIS von Skript 2 (sparse Adjazenzmatrix des auf
    Mannheim geclippten, genutzten Graphen). EINGABE für
    Skript 3.

node_indexStadtradeln2023.csv
    ERGEBNIS von Skript 2 (Zuordnung Matrix-Index <-> OSM-Knoten-ID).
    EINGABE für Skript 3.

NetworkxGraphmetrikenBerechnenStadtradeln2023.py
    SKRIPT 3. Berechnet aus Adjazenzmatrix + Knotenindex die
    Graphmetriken je Knoten. Details siehe unten.

Stadtradeln2023nodes_OSRMwithmetrics.csv
    ENDERGEBNIS von Skript 3. Knotenliste mit Koordinaten,
    Degree, generalisiertem Degree und Betweenness-Zentralität.

----------------------------------------------------------------
ABLAUF DER SKRIPTE
----------------------------------------------------------------

1. stadtradeln2023_osrm_heatmap.py
   - Liest stadtradeln_2023.xlsx, fasst identische Start-Ziel-
     Paare zusammen.
   - Schickt jede einzigartige Verbindung parallel an den
     öffentlichen OSRM-Dienst (routing.openstreetmap.de).
   - Cached die Antworten in cache/stadtradeln2023_osrm_routes.json.
   - Speichert das Ergebnis als GeoJSON
     (stadtradeln2023_osrm_routes.geojson) und zusätzlich als
     interaktive Folium-Karte (stadtradeln2023_osrm_heatmap_new.html).

2. Stadtradeln2023Grapherzeugen.py
   - Liest stadtradeln2023_osrm_routes.geojson aus Schritt 1.
   - Nutzt einen großen Graphen mit 15 km Puffer um Mannheim
     (Matching) sowie den kleinen, exakten Mannheim-Graphen
     (finales Clipping).
   - Matched jede Route per Mittelpunkt-Verfahren auf die
     nächste Kante (Cache
     edge_matching_cache_Stadtradeln2023.csv).
   - Erzeugt eine Heatmap (mannheim_Stadtradeln2023_heatmapOSMR.png).
   - Entfernt ungenutzte Kanten sowie Knoten außerhalb der
     Mannheimer Stadtgrenze.
   - Baut die Adjazenzmatrix ->
     adjacency_matrixStadtradeln2023.npz +
     node_indexStadtradeln2023.csv.

3. NetworkxGraphmetrikenBerechnenStadtradeln2023.py
   - Lädt adjacency_matrixStadtradeln2023.npz und
     node_indexStadtradeln2023.csv aus Schritt 2.
   - Rekonstruiert daraus einen networkx-Subgraphen (Selfloops
     und isolierte Knoten werden entfernt).
   - Berechnet je Knoten: Degree, generalisierten Degree und
     Betweenness-Zentralität.
   - Schreibt das Ergebnis nach
     Stadtradeln2023nodes_OSRMwithmetrics.csv.

----------------------------------------------------------------
HINWEISE
----------------------------------------------------------------

- Identischer Aufbau wie ../2022; ../2024 enthält zusätzlich
  eine erweiterte, logarithmisch skalierte Heatmap-Visualisierung
  und hat ein leicht anderes Rohdaten-Schema.

