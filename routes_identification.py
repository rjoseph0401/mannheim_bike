"""
Erzeugt OD-Paare aus touren_Nextbike.csv mit Häufigkeit (count).

Optionen:
- directed=True  -> gerichtete OD-Paare (A -> B ≠ B -> A)
- directed=False -> ungerichtete Paare (A-B = B-A)

Features:
- problematische StationsIDs können ausgeschlossen werden
- Sortierung nach StationsID
- Ausgabe enthält count der jeweiligen Route
"""

import pandas as pd
from pathlib import Path

# --------------------------------------------------
# Einstellungen
# --------------------------------------------------
IN_PATH = Path("touren_Nextbike.csv")
OUT_PATH = Path("od_paare.csv")

directed = True        # False = ungerichtete Paare
remove_loops = True    # Start = Ende entfernen

bad_ids = [-1, 29111804, 556920840, 95252421, 378595862]


# --------------------------------------------------
# Daten laden
# --------------------------------------------------
df = pd.read_csv(IN_PATH)

# problematische StationsIDs entfernen
df = df[
    ~df["AusleihstationID"].isin(bad_ids) &
    ~df["RueckgabestationID"].isin(bad_ids)
].copy()

# --------------------------------------------------
# OD-Paare erzeugen
# --------------------------------------------------
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
})

# --------------------------------------------------
# Ungerichtete Paare
# --------------------------------------------------
if not directed:
    od["a"] = od[["start_id", "end_id"]].min(axis=1)
    od["b"] = od[["start_id", "end_id"]].max(axis=1)

    od = od.assign(
        start_id=od["a"],
        end_id=od["b"]
    ).drop(columns=["a", "b"])

# --------------------------------------------------
# Loops entfernen
# --------------------------------------------------
if remove_loops:
    od = od[od["start_id"] != od["end_id"]]

# --------------------------------------------------
# Aggregation (count)
# --------------------------------------------------
od_pairs = (
    od.groupby([
        "start_id",
        "end_id",
        "start_lat",
        "start_lon",
        "end_lat",
        "end_lon"
    ])
    .size()
    .reset_index(name="count")
)

# --------------------------------------------------
# Sortierung nach StationsID
# --------------------------------------------------
od_pairs = od_pairs.sort_values(["start_id", "end_id"])

# --------------------------------------------------
# speichern
# --------------------------------------------------
od_pairs.to_csv(OUT_PATH, index=False)

print("OD-Paare:", len(od_pairs))
print("Gespeichert:", OUT_PATH)