"""
Erzeugt OD-Paare aus touren_Nextbike.csv mit Häufigkeit

Optionen:
- directed=True  -> gerichtete OD-Paare
- directed=False -> ungerichtete OD-Paare

Features:
- problematische StationsIDs können ausgeschlossen werden
- Fahrten von einer Station zu sich selbst werden ausgeschlossen
- Sortierung nach count
- Ausgabe: od_paare_gerichtet.csv oder od_paare_ungerichtet.csv
"""

import pandas as pd
from pathlib import Path
import numpy as np

IN_PATH = Path("touren_Nextbike.csv")

# False -> ungerichtet, True -> gerichtet
directed = False

# Fahrten mit Start=Ziel entfernt
exclude_same_station_routes = True

# StationsIDs mit Problemen entfernen
bad_ids = [-1, 29111804, 556920840, 95252421, 378595862]

OUT_PATH = Path("od_paare_gerichtet.csv" if directed else "od_paare_ungerichtet.csv")

df = pd.read_csv(IN_PATH)

df = df[
    ~df["AusleihstationID"].isin(bad_ids) &
    ~df["RueckgabestationID"].isin(bad_ids)
].copy()

# Start und Ziel mit Koordinaten extrahieren
od = df[[
    "AusleihstationID",
    "RueckgabestationID",
    "start_lat",
    "start_lon",
    "end_lat",
    "end_lon"
]].rename(columns={
    "AusleihstationID": "start_id",
    "RueckgabestationID": "end_id"
}).copy()

# Start=Ziel als Route entfernen
if exclude_same_station_routes:
    od = od[od["start_id"] != od["end_id"]].copy()

# Für ungerichtete Paare kleinere ID immer nach vorne setzen
if not directed:
    swap_mask = od["start_id"] > od["end_id"]

    new_start_id = np.where(swap_mask, od["end_id"], od["start_id"])
    new_end_id   = np.where(swap_mask, od["start_id"], od["end_id"])

    new_start_lat = np.where(swap_mask, od["end_lat"], od["start_lat"])
    new_start_lon = np.where(swap_mask, od["end_lon"], od["start_lon"])
    new_end_lat   = np.where(swap_mask, od["start_lat"], od["end_lat"])
    new_end_lon   = np.where(swap_mask, od["start_lon"], od["end_lon"])

    od["start_id"] = new_start_id
    od["end_id"] = new_end_id
    od["start_lat"] = new_start_lat
    od["start_lon"] = new_start_lon
    od["end_lat"] = new_end_lat
    od["end_lon"] = new_end_lon

# Gruppiert alle Fahrten mit gleichem Start und Ziel mit Häufigkeit
od_pairs = (
    od.groupby(
        ["start_id", "end_id", "start_lat", "start_lon", "end_lat", "end_lon"],
        dropna=False
    )
    .size()
    .reset_index(name="count")
)

# Häufigste Routen zuerst
od_pairs = od_pairs.sort_values("count", ascending=False).reset_index(drop=True)

# OD-Paare als CSV-Datei speichern
od_pairs.to_csv(OUT_PATH, index=False)

print("OD-Paare:", len(od_pairs))
print("Gespeichert:", OUT_PATH)
#print(od_pairs.head(20))