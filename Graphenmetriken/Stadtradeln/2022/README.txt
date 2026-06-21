================================================================
 STADTRADELN 2022
================================================================

Berechnet Graphmetriken (Degree, generalisierter Degree,
Betweenness-Zentralität) für das Mannheimer Radwegenetz auf
Basis der Stadtradeln-Zähldaten 2022. Im Gegensatz zu den
Nextbike-Ordnern wird das Routing (OSRM) hier selbst durch-
geführt, daher gibt es einen zusätzlichen, vorgelagerten
Schritt (3 Skripte statt 2).

----------------------------------------------------------------
DATEIEN UND IHRE ROLLE IM SKRIPT-PROZESS
----------------------------------------------------------------

!Manche Dateien sind zu groß für den Upload in das Git Repo gewesen, daher wurden diese im zip Ordner Big Files hochgeladen. Dieser muss für einen funktionierenden Code aber entpackt werden.

stadtradeln_2022.xlsx
    EINGABE für Skript 1. Rohdaten der Stadtradeln-Zählungen:
    Start-/Endkoordinaten und Fahrtenanzahl je Verbindung
    (Spalten count, geometry, start_lon, start_lat, end_lon,
    end_lat).

stadtradeln2022_osrm_heatmap.py
    SKRIPT 1. Führt das OSRM-Routing durch und erzeugt die
    Routengeometrien. Details siehe unten.

stadtradeln2022_osrm_routes.geojson
    ERGEBNIS von Skript 1: alle gerouteten Strecken als GeoJSON
    (Geometrie + Anzahl Fahrten je Strecke). EINGABE für
    Skript 2.

mannheim_bike.graphml
    Radwegenetz Mannheim (kleiner Graph, exakte Stadtgrenze) -
    Cache für Skript 2 und 3. Ein zusätzlicher "großer" Graph
    mit 15 km Puffer (mannheim_bike_large.graphml) wird von
    Skript 2 bei Bedarf automatisch neu erzeugt, falls nicht
    vorhanden.

Stadtradeln2022Grapherzeugen.py
    SKRIPT 2. Matched Routen auf Graph-Kanten, clippt auf
    Mannheimer Stadtgebiet, baut Adjazenzmatrix. Details siehe
    unten.

edge_matching_cache_Stadtradeln2022.csv
    ZWISCHENERGEBNIS von Skript 2. Zuordnung jeder Route zu
    Graph-Kanten (Spalten u, v, k, route_id). Cache für
    wiederholte Läufe.

adjacency_matrixStadtradeln2022.npz
    ERGEBNIS von Skript 2 (sparse Adjazenzmatrix des auf
    Mannheim geclippten, genutzten Graphen). EINGABE für
    Skript 3.

node_indexStadtradeln2022.csv
    ERGEBNIS von Skript 2 (Zuordnung Matrix-Index <-> OSM-Knoten-ID).
    EINGABE für Skript 3.

NetworkxGraphmetrikenBerechnenStadtradeln2022.py
    SKRIPT 3. Berechnet aus Adjazenzmatrix + Knotenindex die
    Graphmetriken je Knoten. Details siehe unten.

Stadtradeln2022nodes_OSRMwithmetrics.csv
    ENDERGEBNIS von Skript 3. Knotenliste mit Koordinaten,
    Degree, generalisiertem Degree und Betweenness-Zentralitaet.

----------------------------------------------------------------
ABLAUF DER SKRIPTE
----------------------------------------------------------------

1. stadtradeln2022_osrm_heatmap.py
   - Liest stadtradeln_2022.xlsx, fasst identische Start-Ziel-
     Paare zusammen.
   - Schickt jede einzigartige Verbindung parallel an den
     öffentlichen OSRM-Dienst (routing.openstreetmap.de).
   - Cached die Antworten in cache/stadtradeln2022_osrm_routes.json
     (verhindert erneute Abfragen bei wiederholtem Lauf).
   - Speichert das Ergebnis als GeoJSON
     (stadtradeln2022_osrm_routes.geojson) und zusätzlich als
     interaktive Folium-Karte (stadtradeln2022_osrm_heatmap_new.html).

2. Stadtradeln2022Grapherzeugen.py
   - Liest stadtradeln2022_osrm_routes.geojson aus Schritt 1.
   - Nutzt zwei Graphen: einen großen Graphen mit 15 km Puffer
     um Mannheim (für das Matching auch außerhalb liegender
     Routenabschnitte) und den kleinen, exakten Mannheim-Graphen
     (zum finalen Clipping).
   - Matched jede Route per Mittelpunkt-Verfahren auf die
     nächste Kante des großen Graphen
     (Cache edge_matching_cache_Stadtradeln2022.csv).
   - Erzeugt eine Heatmap (mannheim_Stadtradeln2022_heatmapOSMR.png).
   - Entfernt ungenutzte Kanten sowie alle Knoten außerhalb der
     Mannheimer Stadtgrenze (per ox.geocode_to_gdf).
   - Baut aus dem verbleibenden Graphen die Adjazenzmatrix ->
     adjacency_matrixStadtradeln2022.npz +
     node_indexStadtradeln2022.csv.

3. NetworkxGraphmetrikenBerechnenStadtradeln2022.py
   - Lädt adjacency_matrixStadtradeln2022.npz und
     node_indexStadtradeln2022.csv aus Schritt 2.
   - Rekonstruiert daraus einen networkx-Subgraphen (Selfloops
     und isolierte Knoten werden entfernt).
   - Berechnet je Knoten: Degree, generalisierten Degree und
     Betweenness-Zentralität.
   - Schreibt das Ergebnis nach
     Stadtradeln2022nodes_OSRMwithmetrics.csv.

----------------------------------------------------------------
HINWEISE
----------------------------------------------------------------

- Identischer Aufbau wie ../2023; ../2024 enthält zusätzlich
  eine erweiterte, logarithmisch skalierte Heatmap-Visualisierung
  und hat ein leicht anderes Rohdaten-Schema.

