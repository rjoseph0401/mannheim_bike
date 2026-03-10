import json
import pandas as pd
from pathlib import Path

IN_GEOJSON = Path("outputs/all_routes_graphhopper_local.geojson")
OUT_CSV = Path("routes_graphhopper.csv")

with open(IN_GEOJSON, "r", encoding="utf-8") as f:
    data = json.load(f)

rows = []

for i, feat in enumerate(data["features"]):

    geom = feat.get("geometry")
    props = feat.get("properties", {})

    if geom is None:
        continue

    coords = geom["coordinates"]

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

df.to_csv(OUT_CSV, index=True)

print("CSV gespeichert:", OUT_CSV)