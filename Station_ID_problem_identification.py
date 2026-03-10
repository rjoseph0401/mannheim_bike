"""
Problem: Inkonsistente Stations-IDs und Koordinaten in den Nextbike-Daten.

Einige Stations-IDs treten im Datensatz mit vielen verschiedenen Koordinaten auf.
Das führt dazu, dass eine einzelne Stations-ID scheinbar an mehreren Orten liegt.
Solche Fälle verfälschen die Identifikation der tatsächlichen Stationen und
führen zu falschen Origin-Destination-Paaren.

Betroffene IDs (Beispiele aus der Datenprüfung):

- -1            → unbekannt
- 29111804      → S-Bahnhof Arena/Maimarkt
- 95252421      → Maimarkt
- 378595862     → Seckenheim - Bahnhof
- 556920840     → unbekannt

Diese IDs werden daher vollständig aus dem Datensatz entfernt
(Start- und Zielstationen).

Anschließend wird überprüft, ob noch Koordinaten existieren,
die mehreren Stations-IDs zugeordnet sind.
"""

import pandas as pd
from pathlib import Path

IN_PATH = Path("touren_Nextbike.csv")

df = pd.read_csv(IN_PATH)

# Problematische StationsIDs 
bad_ids = [-1, 29111804, 556920840, 95252421, 378595862]

# Fahrten entfernen, bei denen Start oder Ziel eine problematische ID hat
df_clean = df[
    ~df["AusleihstationID"].isin(bad_ids) &
    ~df["RueckgabestationID"].isin(bad_ids)
].copy()

# Koordinaten sicher als numerisch interpretieren
for c in ["start_lat", "start_lon", "end_lat", "end_lon"]:
    df_clean[c] = pd.to_numeric(df_clean[c], errors="coerce")

starts = df_clean[["AusleihstationID", "start_lat", "start_lon"]].rename(columns={
    "AusleihstationID": "id",
    "start_lat": "lat",
    "start_lon": "lon"
})

ends = df_clean[["RueckgabestationID", "end_lat", "end_lon"]].rename(columns={
    "RueckgabestationID": "id",
    "end_lat": "lat",
    "end_lon": "lon"
})

# Start- und Zielstationen zusammenführen
stations = pd.concat([starts, ends], ignore_index=True)

# fehlende Werte entfernen und Duplikate löschen
stations = stations.dropna(subset=["id", "lat", "lon"]).drop_duplicates()

# Prüfen, welche Koordinaten mehreren StationsIDs zugeordnet sind
coords_to_ids = (
    stations.groupby(["lat", "lon"])["id"]
    .agg(lambda x: sorted(set(x)))
    .reset_index()
)

coords_to_ids["n_ids"] = coords_to_ids["id"].apply(len)

# Fälle mit mehreren IDs pro Koordinate
mehrere_ids = coords_to_ids[coords_to_ids["n_ids"] > 1].copy()

print("Koordinaten mit mehreren Stations-IDs:")
print(mehrere_ids[["lat", "lon", "n_ids", "id"]].to_string(index=False))

# Routen an bad IDs zählen + Stationsnamen ausgeben

if bad_ids:
    rows = []

    for bid in bad_ids:
        start_count = int((df["AusleihstationID"] == bid).sum())
        end_count = int((df["RueckgabestationID"] == bid).sum())
        total_count = start_count + end_count

        start_names = df.loc[df["AusleihstationID"] == bid, "AusleihstationName"].dropna().unique().tolist()
        end_names = df.loc[df["RueckgabestationID"] == bid, "RueckgabestationName"].dropna().unique().tolist()

        names = sorted(set(start_names + end_names))

        rows.append({
            "bad_id": bid,
            "start_count": start_count,
            "end_count": end_count,
            "total_count": total_count,
            "stationsnamen": names
        })

    bad_id_summary = pd.DataFrame(rows).sort_values("total_count", ascending=False)

    print("\nRouten mit problematischen Stations-IDs:")
    print(bad_id_summary.to_string(index=False))
else:
    print("\nKeine bad_ids gesetzt.")