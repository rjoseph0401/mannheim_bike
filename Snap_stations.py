import time
import requests
import pandas as pd
from pathlib import Path

GH_LOCAL = "http://localhost:8989"

IN_PATH = Path(r"C:\Users\kilia\OneDrive\Dokumente\Uni Mannheim\FSS 2026\Seminar Modellierung und Simulation\mannheim_bike\od_paare_locations.csv")
OUT_STATIONS = Path(r"C:\Users\kilia\OneDrive\Dokumente\Uni Mannheim\FSS 2026\Seminar Modellierung und Simulation\mannheim_bike\stations_snapped.csv")

SLEEP_BETWEEN_REQUESTS = 0.01

# -------------------------
# OD-Paare laden
# -------------------------
df = pd.read_csv(IN_PATH)

for c in ["start_lat", "start_lon", "end_lat", "end_lon"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

df = df.dropna(subset=["start_lat", "start_lon", "end_lat", "end_lon"]).copy()

# -------------------------
# Eindeutige Stationen aus Start- und Endpunkten bauen
# -------------------------
starts = df[["start_lat", "start_lon"]].rename(columns={"start_lat": "lat", "start_lon": "lon"})
ends = df[["end_lat", "end_lon"]].rename(columns={"end_lat": "lat", "end_lon": "lon"})

stations = pd.concat([starts, ends], ignore_index=True).drop_duplicates().reset_index(drop=True)

print("Eindeutige Stationen:", len(stations))

# -------------------------
# Helper: einen Punkt snappen
# -------------------------
def snap_point(lat, lon):
    r = requests.get(
        f"{GH_LOCAL}/nearest",
        params={"point": f"{lat},{lon}"},
        timeout=30
    )
    r.raise_for_status()
    data = r.json()

    # GraphHopper gibt coordinates als [lon, lat] zurück
    snap_lon, snap_lat = data["coordinates"]
    snap_dist_m = data.get("distance", None)

    return snap_lat, snap_lon, snap_dist_m

# -------------------------
# Alle Stationen snappen
# -------------------------
snap_lats = []
snap_lons = []
snap_dists = []
errors = []

for i, row in stations.iterrows():
    lat = float(row["lat"])
    lon = float(row["lon"])

    try:
        snap_lat, snap_lon, snap_dist_m = snap_point(lat, lon)

        snap_lats.append(snap_lat)
        snap_lons.append(snap_lon)
        snap_dists.append(snap_dist_m)
        errors.append(None)

    except Exception as ex:
        snap_lats.append(None)
        snap_lons.append(None)
        snap_dists.append(None)
        errors.append(str(ex))

    if SLEEP_BETWEEN_REQUESTS > 0:
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    if (i + 1) % 50 == 0 or (i + 1) == len(stations):
        print(f"[{i+1}/{len(stations)}]")

stations["snap_lat"] = snap_lats
stations["snap_lon"] = snap_lons
stations["snap_dist_m"] = snap_dists
stations["error"] = errors

stations.to_csv(OUT_STATIONS, index=False, encoding="utf-8-sig")
print("Gespeichert:", OUT_STATIONS)

print("\nSnap-Statistik:")
print(stations["snap_dist_m"].describe())

print("\nSchlechteste 10 Snaps:")
print(stations.sort_values("snap_dist_m", ascending=False).head(10))