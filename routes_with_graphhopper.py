import os
import json
import time
import requests
import pandas as pd
import folium
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

"""
Dieser Code erstellt für alle Paare an od_paaren einen Pfad mit Hilfe von Graphhopper.
Graphhopper wird dabei lokal in einem Docker Container auf http://localhost:8989 betrieben, um Online-API Abfragen zu umgehen.
Das Skript fragt für jedes Paar die Routing-API ab, speichert die Ergebnisse als geojson und erzeugt eine Folium Karte.

Wichtig:
Die Original-Startkoordinate wird am Anfang der Geometrie ergänzt,
die Original-Endkoordinate wird am Ende der Geometrie ergänzt.
Dadurch sieht man im GeoJSON auch die Snap-Strecken von der Station zum Netz.
"""

GH_LOCAL = "http://localhost:8989"

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT
OUT_DIR = ROOT / "outputs"
OUT_DIR.mkdir(exist_ok=True)

# Dateien laden/speichern
IN_PATH = DATA_DIR / "od_paare_ungerichtet.csv"
OUT_GEOJSON = OUT_DIR / "all_routes_graphhopper_local.geojson"
OUT_HTML = OUT_DIR / "all_routes_graphhopper_local.html"

# Möglichkeit nur die Top-N Pfade anzuzeigen, um die Folium Karte stabiler zu halten, um alle anzuzeigen setze None
MAP_TOP_N = None
PROFILE = "bike"  # Alternativ car oder foot

# Throttling der API-Anfragen in Sekunden
SLEEP_BETWEEN_REQUESTS = 0.01

# Zwischenstände werden alle N Routen in geojson abgespeichert, 0 = aus
CHECKPOINT_EVERY = 200


def make_session():
    """
    Erzeugt gebündelte Umgebung für HTTP Abfragen mit Retry bei temporären Fehlern
    """
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def gh_route_geojson_feature(session, start_latlon, end_latlon, count, start_id, end_id, profile=PROFILE):
    """
    Anfrage in der Session für ein Paar, Ausgabe eines rohen geojson Features
    """
    (s_lat, s_lon) = start_latlon
    (e_lat, e_lon) = end_latlon

    params = {
        "point": [f"{s_lat},{s_lon}", f"{e_lat},{e_lon}"],
        "profile": profile,
        "points_encoded": "false",
        "snap_preventions": ["bridge", "tunnel", "ferry", "ford"],
    }

    r = session.get(f"{GH_LOCAL}/route", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    path = data["paths"][0]
    coords_lonlat = path["points"]["coordinates"].copy()  # [lon, lat]

    # Originale Stationspunkte vorne und hinten an die Geometrie anhängen
    coords_lonlat.insert(0, [s_lon, s_lat])
    coords_lonlat.append([e_lon, e_lat])

    # Tatsächlich gesnappte Start-/Endpunkte von Graphhopper
    snapped = path.get("snapped_waypoints")
    snapped_coords = []

    if isinstance(snapped, dict):
        snapped_coords = snapped.get("coordinates", []) or []

    snap_start = snapped_coords[0] if len(snapped_coords) >= 1 else None
    snap_end = snapped_coords[1] if len(snapped_coords) >= 2 else None

    return {
        "type": "Feature",
        "geometry": {"type": "LineString", "coordinates": coords_lonlat},
        "properties": {
            "count": int(count),
            "start_id": int(start_id),
            "end_id": int(end_id),
            "distance_m": float(path["distance"]),
            "duration_s": float(path["time"]) / 1000.0,
            "engine": "graphhopper-local",
            "profile": profile,

            # Originalpunkte
            "input_start_lat": float(s_lat),
            "input_start_lon": float(s_lon),
            "input_end_lat": float(e_lat),
            "input_end_lon": float(e_lon),

            # Gesnappte Punkte von Graphhopper
            "snapped_start_lon": None if snap_start is None else float(snap_start[0]),
            "snapped_start_lat": None if snap_start is None else float(snap_start[1]),
            "snapped_end_lon": None if snap_end is None else float(snap_end[0]),
            "snapped_end_lat": None if snap_end is None else float(snap_end[1]),
        },
    }


def save_geojson(features, out_path):
    """
    Liste von geojson Features als FeatureCollection speichern
    """
    fc = {"type": "FeatureCollection", "features": features}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False)
    return fc


def main():
    """
    Dieser Codeblock liest die Paare von Start und Ende und ruft für jedes Paar über die Session Graphhopper ab.
    Der Fortschritt wird alle 50 erfolgreich gelesenen Pfade angegeben, eine Aussicht für noch verbleibende Dauer wird berechnet.
    """
    df = pd.read_csv(IN_PATH)

    required = {"start_id", "end_id", "start_lat", "start_lon", "end_lat", "end_lon", "count"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Fehlende Spalten in CSV: {missing}. Vorhanden: {list(df.columns)}")

    # -------------------------
    # Datenbereinigung
    # -------------------------

    # Koordinaten numerisch machen
    for c in ["start_lat", "start_lon", "end_lat", "end_lon", "count", "start_id", "end_id"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Zeilen mit fehlenden Koordinaten entfernen
    df = df.dropna(subset=["start_lat", "start_lon", "end_lat", "end_lon", "count", "start_id", "end_id"])

    # Paare entfernen bei denen Start = Ende
    loops = (df["start_lat"] == df["end_lat"]) & (df["start_lon"] == df["end_lon"])
    print("Loops entfernt:", int(loops.sum()))
    df = df[~loops].copy()

    # Sortieren nach Häufigkeit der Routen
    df = df.sort_values("count", ascending=False).reset_index(drop=True)

    session = make_session()

    features = []
    n = len(df)
    t0 = time.time()

    for i, row in df.iterrows():
        s = (float(row["start_lat"]), float(row["start_lon"]))
        e = (float(row["end_lat"]), float(row["end_lon"]))
        c = int(row["count"])
        sid = int(row["start_id"])
        eid = int(row["end_id"])

        try:
            feat = gh_route_geojson_feature(session, s, e, c, sid, eid, profile=PROFILE)
            feat["properties"]["rank"] = i + 1
            features.append(feat)

        except Exception as ex:
            features.append({
                "type": "Feature",
                "geometry": None,
                "properties": {
                    "rank": i + 1,
                    "count": c,
                    "start_id": sid,
                    "end_id": eid,
                    "start": s,
                    "end": e,
                    "error": str(ex),
                    "engine": "graphhopper-local",
                    "profile": PROFILE,
                },
            })

        if SLEEP_BETWEEN_REQUESTS > 0:
            time.sleep(SLEEP_BETWEEN_REQUESTS)

        if (i + 1) % 50 == 0 or (i + 1) == n:
            elapsed = time.time() - t0
            per = elapsed / (i + 1)
            eta = per * (n - (i + 1))
            print(f"[{i+1}/{n}] elapsed={elapsed/60:.1f} min | {per:.3f} s/route | ETA={eta/60:.1f} min")

        if CHECKPOINT_EVERY and (i + 1) % CHECKPOINT_EVERY == 0:
            save_geojson(features, OUT_GEOJSON)
            print("Checkpoint saved:", OUT_GEOJSON)

    fc = save_geojson(features, OUT_GEOJSON)
    print("Saved GeoJSON:", os.path.abspath(OUT_GEOJSON))

    # Folium-Karte erzeugen
    m = folium.Map(location=[49.487, 8.466], zoom_start=13, tiles="OpenStreetMap")

    feats_ok = [f for f in fc["features"] if f.get("geometry") and f["geometry"].get("coordinates")]

    if MAP_TOP_N is not None:
        feats_ok = feats_ok[:MAP_TOP_N]

    for feat in feats_ok:
        coords_latlon = [(lat, lon) for lon, lat in feat["geometry"]["coordinates"]]
        p = feat["properties"]
        folium.PolyLine(
            coords_latlon,
            weight=4,
            opacity=0.8,
            popup=(
                f"Rank {p.get('rank')} | "
                f"start_id={p.get('start_id')} | "
                f"end_id={p.get('end_id')} | "
                f"count={p.get('count')} | "
                f"{p.get('distance_m', 0)/1000:.2f} km | "
                f"{p.get('duration_s', 0)/60:.1f} min"
            ),
        ).add_to(m)

    m.save(OUT_HTML)
    print("Saved HTML:", os.path.abspath(OUT_HTML))


if __name__ == "__main__":
    main()


"""
Code für Powershell, um Docker Umgebung zu starten:
docker run --rm -p 8989:8989 -p 8990:8990 `
  -e "JAVA_OPTS=-Xmx6g -Xms2g" `
  -v C:\gh_local\data:/data `
  --entrypoint /bin/bash `
  israelhikingmap/graphhopper:latest `
  -lc "java -Ddw.graphhopper.datareader.file=/data/planet_8.177,49.324_8.825,49.618.osm.pbf -Ddw.graphhopper.graph.location=/data/graph-cache -jar *.jar server /data/config.yml"

Code für Powershell, um Docker Umgebung zu beenden:
docker stop $(docker ps -q)
"""