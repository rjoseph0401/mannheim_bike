import json
import pandas as pd
from pathlib import Path

DATA_DIR = Path("Data")

IN_GEOJSON = DATA_DIR / "outputs" / "all_routes_graphhopper_local.geojson"
OUT_CSV = DATA_DIR / "routes_graphhopper.csv"

# Optional: sicherstellen, dass Output-Ordner existiert
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

with open(IN_GEOJSON, "r", encoding="utf-8") as f:
    data = json.load(f)

rows = []

for i, feat in enumerate(data["features"]):

    geom = feat.get("geometry")
    props = feat.get("properties", {})

    if geom is None:
        continue

    coords = geom["coordinates"]

    # Safety check (wichtig bei fehlerhaften Routen)
    if not coords or len(coords) < 2:
        continue

    start_lon, start_lat = coords[0]
    end_lon, end_lat = coords[-1]

    rows.append({
        "GraphhopperKoordinaten": coords,
        "startpoint_lat": start_lat,
        "startpoint_lon": start_lon,
        "endpoint_lat": end_lat,
        "endpoint_lon": end_lon,
        "StartStationID": props.get("start_id"),
        "EndStationID": props.get("end_id"),
        "count": props.get("count")
    })

df = pd.DataFrame(rows)

df.to_csv(OUT_CSV, index=False)   # index=False ist hier sauberer

print(f"CSV gespeichert: {OUT_CSV}")
print(f"Anzahl Routen: {len(df)}")