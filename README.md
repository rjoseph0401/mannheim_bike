# Fahrradverkehr Mannheim – Analysen

Seminarprojekt „Modellierung und Simulation" | Universität Mannheim | FSS 2026  
Autoren: Kilian Bonfert · Radek Joseph · Martin Broske

## Projektbeschreibung

Dieses Repository enthält den gesamten Analysecode zur mathematischen Modellierung
des Fahrradverkehrs in Mannheim. Grundlage bilden drei Datensätze:

| Datensatz | Datei(en) | Beschreibung |
|---|---|---|
| **Nextbike** | `Data/touren_Nextbike.csv` | 359 806 Leihradfahrten (Start/Ziel-Stationen) |
| **Stadtradeln** | `Data/stadtradeln_20{22,23,24}.xlsx` | GPS-Segmente der Wettbewerbsjahre 2022–2024 |
| **Dauerzählstellen** | Referenzwerte in `query_locations.py` | 12 stationäre Zähler der Stadt Mannheim |

Das Straßennetzwerk basiert auf OpenStreetMap und wird über `osmnx` geladen bzw. als
gecachter Graph in `Data/mannheim_bike.graphml` gespeichert.

Routenrekonstruktion (für Nextbike) sowie Heatmap-Generierung (für Stadtradeln)
erfolgen über eine lokale GraphHopper-Instanz im Docker-Container
(`http://localhost:8989`).

---

## Verzeichnisstruktur

```
mannheim_bike/
├── Data/
│   ├── mannheim_bike.graphml          # OSM-Graph Mannheim (gecacht)
│   ├── touren_Nextbike.csv            # Nextbike-Rohdaten
│   ├── stadtradeln_2022/23/24.xlsx    # Stadtradeln-Rohdaten
│   ├── od_paare_{gerichtet,ungerichtet}.csv  # OD-Paare für Routing
│   ├── stations_snapped.csv           # Nextbike-Stationen auf Netz gesnapped
│   ├── routes_graphhopper.csv         # Geroutete Nextbike-Routen (CSV)
│   ├── nextbike_graphhopper_routes.gpkg       # Nextbike-Routen (GeoPackage)
│   ├── stadtradeln_graphhopper_routes.gpkg    # Stadtradeln-Routen (GeoPackage)
│   ├── vergleich_edge_stats.csv       # Kantenbelegung NB vs. SR
│   └── outputs/
│       └── stadtradeln_graphhopper_routes_{Jahr}.gpkg
├── Results/                           # Alle Ausgabe-Plots und -Tabellen
├── outputs/                           # GeoJSON/HTML Rohausgaben (Routing)
├── *.py                               # Analyseskripte (siehe unten)
├── requirements.txt
└── README.md
```

---

## Skripte & Ausführungsreihenfolge

Die Skripte bauen aufeinander auf. Die empfohlene Reihenfolge:

### 1. Routing & Datenerzeugung

| Skript | Beschreibung | Erzeugt |
|---|---|---|
| `routes_with_graphhopper.py` | Ruft GraphHopper-API für alle Nextbike-OD-Paare auf (Docker nötig). Speichert GeoJSON und Folium-Karte. | `outputs/all_routes_graphhopper_local.{geojson,html}` |
| `stadtradeln_graphhopper_heatmap.py` | Verarbeitet Stadtradeln-Rohdaten (xlsx), ruft GraphHopper auf, erzeugt geroutete Heatmaps je Jahr. Jahrgang über `INPUT_FILE` wählen. | `Data/outputs/stadtradeln_graphhopper_routes_{Jahr}.gpkg`, `Results/graphhopper_stadtradeln{Jahr}_heatmap_*.png` |
| `geojson_to_csv.py` | Konvertiert das Nextbike-GeoJSON in eine CSV mit Kantengewichten. | `Data/routes_graphhopper.csv` |

### 2. Graphen & Heatmaps

| Skript | Beschreibung | Erzeugt |
|---|---|---|
| `graph_generation.py` | Lädt OSM-Graph, projiziert Nextbike-Routen auf Kanten (Soft-Matching, 100 m Radius), gewichtet nach Fahrtenzahl; erzeugt Heatmap-Plot (Anteil- oder Perzentil-Skala). | `mannheim_graphhopper_heatmap_{share\|percentile}.png` (Arbeitsverzeichnis) |
| `stadtradeln_graph_generation.py` | Analoges Skript für Stadtradeln-Routen: Soft-Matching, Kalibrierung, drei Darstellungsmodi (city_classes / share / percentile). Jahrgang über `RUN_TAG` wählen. | `Results/graphhopper_stadtradeln{22,23,24}_heatmap_*.png` |
| `stadtradeln_top_routes_map.py` | Karte der Top-15 meistgefahrenen Stadtradeln-Segmente je Jahr. | `Results/stadtradeln_top15_routen_{Jahr}.png` |

### 3. Vergleich & Validierung

| Skript | Beschreibung | Erzeugt |
|---|---|---|
| `comparison_nextbike_stadtradeln.py` | Vergleicht Kantenbelegung zwischen Nextbike- und Stadtradeln-Daten; erzeugt Differenz-Heatmaps. | `Results/vergleich_diff_stadtradeln_minus_nextbike.png` |
| `comparison_stadtradeln_intern.py` | Jahresvergleich innerhalb der Stadtradeln-Daten (2022 vs. 2023, 2022 vs. 2024 usw.); prozentuale Veränderungen. | `Results/delta_*_{percent_change,share}.png` |
| `stadtradeln_analysis.py` | Statische Analysen: Gesamtkilometer je Jahr (gewichtet). | `Results/stadtradeln_gesamtkilometer.png` |
| `query_locations.py` | Querungsanalyse an den 12 Dauerzählstellen: Transekt-Schnittpunkte der Stadtradeln-Routen, Vergleich mit Referenzzählungen. | `Results/fahrtenzahlen_abfrage.csv` |
| `plot_transect_sensitivity.py` | Sensitivitätsanalyse der Transektbreite an den drei Problemzählstellen (B38, Neckarauer Übergang, Theodor-Heuss-Anlage). | `Results/transect_sensitivity.png` |
| `validate_locations.py` | Interaktive Folium-Karte aller Transekte zur visuellen Kontrolle. | `Results/validate_locations.html` |

---

## Voraussetzungen

```bash
pip install -r requirements.txt
```

Für das Routing (Nextbike und Stadtradeln) wird GraphHopper als lokaler
Docker-Container benötigt:

```bash
docker run -d -p 8989:8989 graphhopper/graphhopper
```