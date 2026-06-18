import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import json

import folium
import geopandas as gpd
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from matplotlib.colors import LogNorm

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/

# Ordner mit stadtradeln_YYYY.xlsx. Override mit Umgebungsvariable DATA_INPUT_DIR.
INPUT_DIR = Path(os.environ.get("DATA_INPUT_DIR", DATA_DIR.parent / "download"))

YEARS = [2022, 2023, 2024]
CACHE_DIR = DATA_DIR / "cache"
ROUTE_CACHE_FILE = CACHE_DIR / "stadtradeln_osrm_routes.json"


def key_of(slon, slat, elon, elat):
    return f"{slon},{slat};{elon},{elat}"


def load_pairs(year):
    parquet = CACHE_DIR / f"stadtradeln_{year}.parquet"
    excel = INPUT_DIR / f"stadtradeln_{year}.xlsx"
    if parquet.exists():
        df = pd.read_parquet(parquet)
    else:
        df = pd.read_excel(excel)
        df.columns = df.columns.str.strip().str.lower().str.replace(r"\s+", "_", regex=True)

    if not {"slon", "slat", "elon", "elat", "number_of_matched_trips"}.issubset(df.columns):
        if {"x_start", "y_start", "x_end", "y_end", "number_of_matched_trips"}.issubset(df.columns):
            for c in ["x_start", "y_start", "x_end", "y_end", "number_of_matched_trips"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.dropna(subset=["x_start", "y_start", "x_end", "y_end", "number_of_matched_trips"])
            s = gpd.GeoDataFrame(geometry=gpd.points_from_xy(df.x_start, df.y_start), crs="EPSG:25832").to_crs(4326)
            e = gpd.GeoDataFrame(geometry=gpd.points_from_xy(df.x_end, df.y_end), crs="EPSG:25832").to_crs(4326)
            df["slon"], df["slat"] = s.geometry.x.values, s.geometry.y.values
            df["elon"], df["elat"] = e.geometry.x.values, e.geometry.y.values
        elif {"start_lon", "start_lat", "end_lon", "end_lat", "count"}.issubset(df.columns):
            for c in ["start_lon", "start_lat", "end_lon", "end_lat", "count"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.dropna(subset=["start_lon", "start_lat", "end_lon", "end_lat", "count"])
            df = df.rename(
                columns={
                    "start_lon": "slon",
                    "start_lat": "slat",
                    "end_lon": "elon",
                    "end_lat": "elat",
                    "count": "number_of_matched_trips",
                }
            )
        else:
            raise ValueError(f"Unbekanntes Schema in {excel.name}: {list(df.columns)}")

        df = df[["slon", "slat", "elon", "elat", "number_of_matched_trips"]].copy()
        df.to_parquet(parquet)

    pairs = df.groupby(["slon", "slat", "elon", "elat"], as_index=False)["number_of_matched_trips"].sum()
    return pairs[(pairs.slon != pairs.elon) | (pairs.slat != pairs.elat)].copy()


def get_route(session, slon, slat, elon, elat):
    try:
        url = f"https://routing.openstreetmap.de/routed-bike/route/v1/driving/{slon},{slat};{elon},{elat}"
        j = session.get(url, params={"overview": "full", "geometries": "geojson"}, timeout=15).json()
        if j.get("code") == "Ok":
            return j["routes"][0]["geometry"]["coordinates"]
    except Exception:
        pass


def build_heatmap(year, cache, session):
    pairs = load_pairs(year)
    trips_map = {key_of(r.slon, r.slat, r.elon, r.elat): r.number_of_matched_trips for r in pairs.itertuples()}
    todo = [
        (k, r.slon, r.slat, r.elon, r.elat)
        for r in pairs.itertuples()
        for k in [key_of(r.slon, r.slat, r.elon, r.elat)]
        if k not in cache
    ]
    print(f"{year}: {len(todo)} neu, {len(pairs)-len(todo)} aus Cache")

    with ThreadPoolExecutor(max_workers=32) as ex:
        futures = {ex.submit(get_route, session, slon, slat, elon, elat): k for k, slon, slat, elon, elat in todo}
        for i, fut in enumerate(as_completed(futures), 1):
            cache[futures[fut]] = fut.result()
            if i % 1000 == 0 or i == len(todo):
                ROUTE_CACHE_FILE.write_text(json.dumps(cache))

    vals = [trips_map[k] for k, coords in cache.items() if coords and k in trips_map]
    vmin = max(1, int(np.percentile(vals, 5)))
    vmax = int(np.percentile(vals, 99.5))
    vmax = max(vmax, vmin + 1)
    norm = LogNorm(vmin=vmin, vmax=vmax)
    cmap = plt.get_cmap("turbo")

    m = folium.Map(location=[49.487, 8.466], zoom_start=13)
    for k, coords in cache.items():
        if not coords or k not in trips_map:
            continue
        t = trips_map[k]
        folium.PolyLine(
            [[lat, lon] for lon, lat in coords],
            color=mcolors.to_hex(cmap(norm(t))),
            weight=1.5 + 2.5 * t / vmax,
            opacity=0.9,
        ).add_to(m)

    out = DATA_DIR / f"stadtradeln_osrm_heatmap_{year}.html"
    m.save(out)
    print("Gespeichert:", out)


CACHE_DIR.mkdir(exist_ok=True)
cache = json.loads(ROUTE_CACHE_FILE.read_text()) if ROUTE_CACHE_FILE.exists() else {}
session = requests.Session()
session.mount("https://", requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32))

for year in YEARS:
    build_heatmap(year, cache, session)

ROUTE_CACHE_FILE.write_text(json.dumps(cache))
