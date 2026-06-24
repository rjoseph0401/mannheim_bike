# Cache

Seminarprojekt „Modellierung und Simulation" | Universität Mannheim | FSS 2026
Autoren: Kilian Bonfert · Radek Joseph · Martin Broske

## Beschreibung

Dieser Ordner enthält vorberechnete Zwischenergebnisse, damit die Analyse-Skripte
nicht bei jedem Lauf erneut routen oder die Excel-Rohdaten einlesen müssen. Die
Dateien werden von den Skripten in `nextbike/` und `stadtradeln/` automatisch
erzeugt bzw. gelesen.

---

## Inhalt

| Datei | Erzeugt von | Gelesen von | Beschreibung |
|---|---|---|---|
| `nextbike_edge_hits_cache.json` | `nextbike/nextbike_heatmap_kanten.py` | `nextbike/nextbike_heatmap_kanten_anteil.py`, `nextbike/nextbike_heatmap_radfreundlich_vergleich.py`, `nextbike/load_counter_and_nextbike_data.py` | Kantentreffer der Nextbike-Routen auf dem OSM-Graphen, als Liste `[u, v, k, count]` je Kante. |
| `stadtradeln_2022.parquet` | OD-Vorverarbeitung | `stadtradeln/stadtradeln_osrm_heatmap*.py` | Aggregierte Stadtradeln-OD-Paare 2022 (`slon`, `slat`, `elon`, `elat`, `number_of_matched_trips`). |
| `stadtradeln_2023.parquet` | OD-Vorverarbeitung | `stadtradeln/stadtradeln_osrm_heatmap*.py` | Aggregierte Stadtradeln-OD-Paare 2023. |
| `stadtradeln_2024.parquet` | OD-Vorverarbeitung | `stadtradeln/stadtradeln_osrm_heatmap*.py` | Aggregierte Stadtradeln-OD-Paare 2024. |
| `stadtradeln_osrm_routes.json` | `stadtradeln/stadtradeln_osrm_heatmap.py` | `stadtradeln/stadtradeln_osrm_heatmap_jpg.py` | Über OSRM gecachte Routengeometrien je OD-Paar (Schlüssel `slon,slat;elon,elat`). |

---

## Hinweise

Die Cache-Dateien sind reine Zwischenergebnisse und können bei Bedarf gelöscht
werden – die zugehörigen Skripte erzeugen sie beim nächsten Lauf neu (für die
OSRM-Caches ist dann wieder eine erreichbare OSRM-Instanz nötig). Liegen die
`stadtradeln_{Jahr}.parquet`-Dateien vor, werden die `stadtradeln_{Jahr}.xlsx`-
Rohdaten nicht erneut eingelesen.
