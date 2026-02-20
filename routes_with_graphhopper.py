import os
import json
import time
import math
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
"""

GH_LOCAL = "http://localhost:8989"

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUT_DIR  = ROOT / "outputs"
OUT_DIR.mkdir(exist_ok=True)

# Dateien laden/speichern
IN_PATH = DATA_DIR / "od_paare_locations.csv"
OUT_GEOJSON = OUT_DIR / "all_routes_graphhopper_local.geojson"
OUT_HTML    = OUT_DIR / "all_routes_graphhopper_local.html"

# Möglichkeit nur die Top-N Pfade anzuzeigen, um die Folium Karte stabiler zu halten, um alle anzuzeigen setze None
MAP_TOP_N = None
PROFILE = "bike" # Alternativ car oder foot

# Throttling der API-Anfragen in Sekunden
SLEEP_BETWEEN_REQUESTS = 0.01  

# Zwischenstände werden alle N ROuten in geojson abgespeichert, 0 = aus
CHECKPOINT_EVERY = 200


def make_session():
    """
    Erzeugt gebündelte Umgebung für HTTP Abfragen mit Retry bei temporären Fehlern
    """
    # Bündelt HTTP Abfragen
    s = requests.Session() 
    retries = Retry(
        total=5, # Zahl Wiederholung von Verbindungen
        backoff_factor=0.5, # Exponentieller Faktor im Warten zwischen Requests 1s -> 2s -> 4s...
        status_forcelist=[429, 500, 502, 503, 504], # 429: Too Many Requests, 500: Internal Server Error, 502: Bad Gateway, 503: Service Unavailable, 504: Gateway Timeout
                                                    # Bei diesen temporären Fehlern neu versuchen, zB 400 und 404 ignorieren
        allowed_methods=["GET"] # Retry gibt nur eine Instanz wieder ähnlich wie f(f(x)) = f(x)
    )
    # Adapter für Retry
    s.mount("http://", HTTPAdapter(max_retries=retries)) 
    # Wird lokal nicht genutzt, zur Stabilität für mögliche HTTPS Anfragen
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def gh_route_geojson_feature(session, start_latlon, end_latlon, count, profile=PROFILE):
    """
    Anfrage in der Session für ein Paar, Ausgabe eines rohen geojson Features
    """
    # Sammeln Start_latitude, Start_longitude
    (s_lat, s_lon) = start_latlon 
    (e_lat, e_lon) = end_latlon

    params = {
        "point": [f"{s_lat},{s_lon}", f"{e_lat},{e_lon}"], # Graphhopper erwartet /route?point=lat1,lon1&point=lat2,lon2
        "profile": profile, # bike Profil 
        "points_encoded": "false", # geojson Rückgabe
    }

    # GET http://localhost:8989/route?...
    r = session.get(f"{GH_LOCAL}/route", params=params, timeout=30) 
    # Fehlerstatusüberprüfung
    r.raise_for_status() 
    data = r.json() 

    # Graphhopper kann mehrere Routen liefern, wähle die erste
    path = data["paths"][0] 
    coords_lonlat = path["points"]["coordinates"] # [lon, lat]


    return {
        "type": "Feature",
        "geometry": {"type": "LineString", "coordinates": coords_lonlat},
        "properties": {
            "count": int(count), # Häufigkeit des Paars
            "distance_m": float(path["distance"]), # Distanz
            "duration_s": float(path["time"]) / 1000.0, # Dauer
            "engine": "graphhopper-local",
            "profile": profile,
        },
    }

# Liste von geojson Features als FeatureCollection
def save_geojson(features, out_path): 
    fc = {"type": "FeatureCollection", "features": features}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False)
    return fc


def main():
    """
    Dieser Codeblock ließt die Paare von Start und Ende und ruft für jedes Paar über die Session Graphhopper ab.
    Der Fortschritt wird alle 50 erfolgreich gelesenen Pfade angegeben, eine Aussicht für noch verbleibende Dauer wird berechnent.
    Der Output ist eine geojson Datei der Form
    {
  "type": "Feature",
  "geometry": {
    "type": "LineString",
    "coordinates": [
      [8.46612, 49.48701],
      [8.46598, 49.48672],
      ...
    ]
  },
  "properties": {
    "count": 3817,
    "distance_m": 1240.3,
    "duration_s": 312.5,
    "engine": "graphhopper-local",
    "profile": "bike",
    "rank": 1
        }
    }
    """
    df = pd.read_csv(IN_PATH)

    required = {"start_lat", "start_lon", "end_lat", "end_lon", "count"}
    missing = required - set(df.columns)
    # Abbruch, falls Daten für Paare fehlen
    if missing:
        raise ValueError(f"Fehlende Spalten in CSV: {missing}. Vorhanden: {list(df.columns)}")

    # Sortieren nach Häufigkeit der Routen (falls nur die ersten N relevant)
    df = df.sort_values("count", ascending=False).reset_index(drop=True)

    # Beginne Sessin
    session = make_session()

    features = []
    n = len(df)
    t0 = time.time()

    for i, row in df.iterrows():
        s = (float(row["start_lat"]), float(row["start_lon"]))
        e = (float(row["end_lat"]), float(row["end_lon"]))
        c = int(row["count"])

        # feat als geojson FeatureProfile
        try:
            feat = gh_route_geojson_feature(session, s, e, c, profile=PROFILE)
            feat["properties"]["rank"] = i + 1
            features.append(feat)
        except Exception as ex:
            # Fehler protokollieren, aber weiterlaufen
            features.append({
                "type": "Feature",
                "geometry": None,
                "properties": {
                    "rank": i + 1,
                    "count": c,
                    "start": s,
                    "end": e,
                    "error": str(ex),
                    "engine": "graphhopper-local",
                    "profile": PROFILE,
                },
            })

        if SLEEP_BETWEEN_REQUESTS > 0:
            time.sleep(SLEEP_BETWEEN_REQUESTS)

        # Fortschritt zur Anzeige
        if (i + 1) % 50 == 0 or (i + 1) == n:
            elapsed = time.time() - t0
            per = elapsed / (i + 1)
            eta = per * (n - (i + 1))
            print(f"[{i+1}/{n}] elapsed={elapsed/60:.1f} min | {per:.3f} s/route | ETA={eta/60:.1f} min")

        # Checkpoint zum Zwischenspeichern
        if CHECKPOINT_EVERY and (i + 1) % CHECKPOINT_EVERY == 0:
            save_geojson(features, OUT_GEOJSON)
            print("Checkpoint saved:", OUT_GEOJSON)

    # Finale geojson Datei
    fc = save_geojson(features, OUT_GEOJSON)
    print("Saved GeoJSON:", os.path.abspath(OUT_GEOJSON))

    # Folium, wird groß für über 500 Pfade (Nextbike_touren hat ca 4500)
    m = folium.Map(location=[49.487, 8.466], zoom_start=13, tiles="OpenStreetMap")

    # Nur Features mit echter Geometrie
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
            popup=f"Rank {p.get('rank')} | count={p.get('count')} | {p.get('distance_m',0)/1000:.2f} km | {p.get('duration_s',0)/60:.1f} min",
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
  -lc "java -Ddw.graphhopper.datareader.file=/data/region.osm.pbf -Ddw.graphhopper.graph.location=/data/graph-cache -jar *.jar server /data/config.yml"

Code für Powershell, um Docker Umgebung zu beenden:
  docker stop $(docker ps -q)
"""