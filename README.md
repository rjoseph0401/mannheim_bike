# mannheim_bike

Datenbasis und Analyse-Skripte für die Modellierung von Fahrradverkehrsströmen in Mannheim.

## Datensätze

### `touren_Nextbike.csv`
Rohdaten aller Nextbike-Leihfahrradfahrten in Mannheim (~79 MB).

| Spalte | Beschreibung |
|---|---|
| `FahrradID` | Eindeutige Fahrzeug-ID |
| `AusleihstationID` / `AusleihstationName` | ID und Name der Startstation |
| `AusleihzeitID` | Zeitstempel der Ausleihe (kodiert) |
| `RueckgabestationID` / `RueckgabestationName` | ID und Name der Rückgabestation |
| `Rueckgabe_datetime` | Rückgabezeitpunkt als Datetime |
| `Rueckgabe_Stunde` / `Rueckgabe_Stunde_des_Tages` | Stunde der Rückgabe |
| `start_lat/lon` / `end_lat/lon` | Koordinaten von Start- und Zielstation |
| `Dauer` | Fahrtdauer |
| `dist_km` | Luftliniendistanz in Kilometern |

### `od_paare_gerichtet.csv`
Aggregierte **gerichtete** Origin-Destination-Paare mit Fahrtenhäufigkeit. A→B und B→A werden separat gezählt.

### `od_paare_ungerichtet.csv`
Aggregierte **ungerichtete** OD-Paare. Fahrten zwischen zwei Stationen werden unabhängig von der Richtung zusammengefasst.

Beide OD-Dateien haben das Schema: `start_id, end_id, start_lat, start_lon, end_lat, end_lon, count`

### `stadtradeln_2022/2023/2024.xlsx`
Stadtradeln-Kampagnendaten der Stadt Mannheim für die Jahre 2022–2024.

## Skripte

### `Station_ID_problem_identification.py`
Identifiziert problematische Stations-IDs im Nextbike-Datensatz.

Einige IDs (z. B. `-1`, `29111804`, `95252421`) tauchen mit inkonsistenten Koordinaten auf und verfälschen die Stationsidentifikation. Das Skript listet betroffene IDs mit Fahrtanzahl und Stationsnamen sowie Koordinaten, denen mehrere IDs zugeordnet sind.

**Problematische IDs:** `-1`, `29111804`, `556920840`, `95252421`, `378595862`

### `routes_identification.py`
Erzeugt OD-Paare aus `touren_Nextbike.csv`.

- Filtert problematische Stations-IDs heraus
- Schließt Fahrten von einer Station zu sich selbst aus
- Wählbar: gerichtete oder ungerichtete Paare (`directed = True/False`)
- Ausgabe: `od_paare_gerichtet.csv` oder `od_paare_ungerichtet.csv`

## Verzeichnisstruktur

```
mannheim_bike/
├── touren_Nextbike.csv              # Nextbike Rohdaten
├── od_paare_gerichtet.csv           # Gerichtete OD-Paare
├── od_paare_ungerichtet.csv         # Ungerichtete OD-Paare
├── stadtradeln_2022/2023/2024.xlsx  # Stadtradeln-Daten
├── Station_ID_problem_identification.py
├── routes_identification.py
├── Data/
├── Results/
└── outputs/
```
