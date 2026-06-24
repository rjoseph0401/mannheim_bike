# Nextbike – Analysen

Seminarprojekt „Modellierung und Simulation" | Universität Mannheim | FSS 2026
Autoren: Kilian Bonfert · Radek Joseph · Martin Broske

## Beschreibung

Dieser Ordner enthält alle Skripte zur Analyse der **Nextbike**-Leihradfahrten.
Grundlage ist die Datei `Data/touren_Nextbike.csv` (359 806 Fahrten mit Start-/
Ziel-Stationen), aus der mit `route_generating_osmr.py` über OSRM Routen
rekonstruiert und in `df_nextbike_merged_mit_routen.csv` abgelegt werden. Die so
erzeugten Routen werden auf das OSM-Straßennetz (`mannheim_bike.graphml`)
projiziert und als Kantentreffer in `cache/nextbike_edge_hits_cache.json`
zwischengespeichert.

Die Skripte erzeugen daraus Heatmaps, Histogramme sowie einen Abgleich mit den
zwölf städtischen Dauerzählstellen.

---

## Skripte & Ausführungsreihenfolge

Die Skripte bauen aufeinander auf. Empfohlene Reihenfolge:

### 1. Routenerzeugung

| Skript | Beschreibung | Erzeugt |
|---|---|---|
| `route_generating_osmr.py` | Routet alle Nextbike-OD-Paare über eine OSRM-Instanz und reichert die Rohfahrten um die rekonstruierten Routen an. | `df_nextbike_merged_mit_routen.csv` |
| `compute_unique_route_counts.py` | Debug-Skript: überprüft das Route-↔-Kanten-Matching für ausgewählte Stationen. | (Konsolenausgabe) |

### 2. Heatmaps & Visualisierung

| Skript | Beschreibung | Erzeugt |
|---|---|---|
| `nextbike_heatmap_kanten.py` | Projiziert alle Routen auf die Netzkanten und erzeugt eine kantenbasierte Heatmap (Log-Skala); schreibt zugleich den Hits-Cache. | `cache/nextbike_edge_hits_cache.json`, Heatmap-PNG |
| `nextbike_heatmap_kanten_anteil.py` | Heatmap auf Basis des Kanten-*Anteils* an allen Fahrten (Log-Norm). Benötigt Graph + Hits-Cache. | `mannheim_nextbike_heatmap_osrm_anteil.png` |
| `nextbike_heatmap_radfreundlich_vergleich.py` | Vergleicht die Kantenbelegung mit den Radinfrastruktur-Tags (`bicycle`, `cycleway`, `surface`) aus OSM. | `nextbike_heatmap_vergleich_radfreundlich.png` |
| `nextbike_compare_heatmap_2024_periods.py` | Interaktive Folium-DualMap: Vergleich zweier Zeitfenster 2024 (Mai–Juni vs. Juli–August). | HTML-Karte |
| `nextbike_compare_heatmap_2024_periods_edges_jpg.py` | Kantenbasierte Variante des Periodenvergleichs als statisches Bild. | JPG-Heatmap |
| `nextbike_compare_heatmap_rain_vs_dry.py` | Vergleicht Fahrtaufkommen an Regen- vs. Trockentagen (Wetterdaten via Open-Meteo); Differenz-Heatmap. | `nextbike_dry_minus_rainy.jpg` |
| `nextbike_histogramm_fahrten_pro_monat.py` | Histogramm der Fahrten je Monat. | `nextbike_fahrten_pro_monat_histogramm.png` |
| `nextbike_histogramm_fahrten_pro_wochentag.py` | Histogramm der Fahrten je Wochentag. | `nextbike_fahrten_pro_wochentag_histogramm.png` |

### 3. Abgleich mit Dauerzählstellen

| Skript | Beschreibung | Erzeugt |
|---|---|---|
| `load_counter_and_nextbike_data.py` | Gemeinsames Lade-Modul: liest die Zählstellen-Excel-Dateien (von GitHub), den OSM-Graphen und den Nextbike-Hits-Cache; definiert Stations-IDs, Radien und Korridor-Logik. | (importiert von den übrigen Skripten) |
| `uebersicht_dauerradzaehler_korridore.py` | Bestimmt je Dauerzählstelle den zugehörigen Netzkorridor (Radius- und Term-Regeln, teils erzwungene Kanten). | `uebersicht_dauerradzaehler_korridore.csv` |
| `map_dauerradzaehler_korridore.py` | Interaktive Folium-Karte der Zählstellen-Korridore zur visuellen Kontrolle. | `dauerradzaehler_korridore_map.html` |
| `dauerzaehl_nextbike_match.py` | Zählt Nextbike-Überfahrten je Zählstelle und bildet sie auf die Korridore ab. | `matched_edges_nextbike_gesamt.csv`, `quotient_nextbike_vs_zaehler.csv` |
| `compute_quotient_nextbike_gegenueber_zaehler.py` | Berechnet den Quotienten Nextbike-Verkehr / Gesamtverkehr für den Nextbike-Zeitraum (2025-03-06 bis 2025-10-06). *Weitgehend durch `dauerzaehl_nextbike_match.py` ersetzt.* | CSV |

---

## Voraussetzungen

```
pip install -r ../requirements.txt
```

Für die Routenrekonstruktion (`route_generating_osmr.py`) wird eine erreichbare
OSRM-Instanz benötigt. `nextbike_compare_heatmap_rain_vs_dry.py` lädt zusätzlich
Wetterdaten über die Open-Meteo-API.
