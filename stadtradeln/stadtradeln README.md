# Stadtradeln – Analysen

Seminarprojekt „Modellierung und Simulation" | Universität Mannheim | FSS 2026
Autoren: Kilian Bonfert · Radek Joseph · Martin Broske

## Beschreibung

Dieser Ordner enthält die Skripte zur Auswertung der **Stadtradeln**-Daten der
Wettbewerbsjahre 2022–2024. Eingabe sind die GPS-Segmente aus
`stadtradeln_20{22,23,24}.xlsx` (Koordinaten in EPSG:25832, Spalte
`number_of_matched_trips` als Gewicht). Für das Routing-basierte Verfahren werden
die als Parquet vorberechneten Paare aus `cache/stadtradeln_{Jahr}.parquet`
verwendet; die gerouteten Strecken liegen im OSRM-Routen-Cache
(`cache/stadtradeln_osrm_routes.json`).

Die Skripte erzeugen Heatmaps der Kantenbelegung sowie eine Statistik der
meistbefahrenen Straßen.

---

## Skripte

| Skript | Beschreibung | Erzeugt |
|---|---|---|
| `stadtradeln_2024_heatmap.py` | Direkte (segmentbasierte) Heatmap für 2024: Segmente werden gesampelt, gewichtet und als Log-Heatmap geplottet. Eingabeordner über `DATA_INPUT_DIR` wählbar. | `stadtradeln_2024_heatmap_robust.png` |
| `stadtradeln_2024_strassen_stats.py` | Aggregiert die gewichteten Fahrten je Straße (auf reduziertem Hauptachsen-Graph `mannheim_bike_hauptachsen.graphml`). | `stadtradeln_2024_strassen_stats_hauptachsen.csv` |
| `stadtradeln_osrm_heatmap.py` | Routet alle Jahres-OD-Paare (2022–2024) über OSRM, cached die Routen und erzeugt interaktive sowie statische Heatmaps. | `cache/stadtradeln_osrm_routes.json`, Heatmap-/Folium-Ausgaben |
| `stadtradeln_osrm_heatmap_jpg.py` | Statische, kantenbasierte Variante: snappt die gerouteten Strecken auf das (ggf. erweiterte) Netz und erzeugt Log-Heatmaps je Jahr als Bild. | JPG-Heatmaps je Jahr |

---

## Voraussetzungen

```
pip install -r ../requirements.txt
```

Die OSRM-basierten Skripte (`stadtradeln_osrm_heatmap.py`,
`stadtradeln_osrm_heatmap_jpg.py`) benötigen beim ersten Lauf eine erreichbare
OSRM-Instanz; danach genügt der Routen-Cache. Liegen die
`cache/stadtradeln_{Jahr}.parquet`-Dateien vor, werden die Excel-Rohdaten nicht
erneut eingelesen.
