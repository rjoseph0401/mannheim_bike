from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import json, numpy as np, geopandas as gpd, pandas as pd
import requests, folium, matplotlib.pyplot as plt, matplotlib.colors as mcolors
from matplotlib.colors import LogNorm
from shapely.geometry import LineString



#Path anpassen!
INPUT_FILE = Path("stadtradeln_2023.xlsx")
PARQUET    = Path("cache/stadtradeln_2023.parquet")
CACHE_FILE = Path("cache/stadtradeln2023_osrm_routes.json")

# Daten laden
PARQUET.parent.mkdir(exist_ok=True)
if not PARQUET.exists():
    df = pd.read_excel(INPUT_FILE)
    df.columns = df.columns.str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
    for c in ["start_lon", "start_lat", "end_lon", "end_lat", "count"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df.dropna(subset=["start_lon", "start_lat", "end_lon", "end_lat", "count"]).to_parquet(PARQUET)
df = pd.read_parquet(PARQUET)

pts_s = gpd.GeoDataFrame(geometry=gpd.points_from_xy(df.start_lon, df.start_lat), crs="EPSG:4326 ").to_crs(4326)
pts_e = gpd.GeoDataFrame(geometry=gpd.points_from_xy(df.end_lon,   df.end_lat),   crs="EPSG:4326 ").to_crs(4326)
df["slon"], df["slat"] = pts_s.geometry.x.values, pts_s.geometry.y.values
df["elon"], df["elat"] = pts_e.geometry.x.values, pts_e.geometry.y.values

pairs = df.groupby(["slon", "slat", "elon", "elat"], as_index=False)["count"].sum()
pairs = pairs[pairs.slon != pairs.elon].copy()
trips_map = {f"{r.slon},{r.slat};{r.elon},{r.elat}": r.count for r in pairs.itertuples()}
print(f"Einzigartige Paare: {len(pairs)}")

# OSRM Routing mit Cache
cache = json.loads(CACHE_FILE.read_text()) if CACHE_FILE.exists() else {}
session = requests.Session()
session.mount("https://", requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32))

def get_route(slon, slat, elon, elat):
    try:
        j = session.get(
            f"https://routing.openstreetmap.de/routed-bike/route/v1/driving/{slon},{slat};{elon},{elat}",
            params={"overview": "full", "geometries": "geojson"}, timeout=15).json()
        if j.get("code") == "Ok":
            return j["routes"][0]["geometry"]["coordinates"]
    except Exception:
        pass

todo = [(f"{r.slon},{r.slat};{r.elon},{r.elat}", r.slon, r.slat, r.elon, r.elat)
        for r in pairs.itertuples() if f"{r.slon},{r.slat};{r.elon},{r.elat}" not in cache]
print(f"{len(todo)} neu, {len(pairs)-len(todo)} aus Cache")

with ThreadPoolExecutor(max_workers=32) as ex:
    futures = {ex.submit(get_route, slon, slat, elon, elat): key for key, slon, slat, elon, elat in todo}
    for i, fut in enumerate(as_completed(futures), 1):
        cache[futures[fut]] = fut.result()
        if i % 1000 == 0 or i == len(todo):
            CACHE_FILE.write_text(json.dumps(cache)); print(f"  {i}/{len(todo)}")


# Routen als GeoDataFrame aufbauen
route_records = []
for key, coords in cache.items():
    if not coords or len(coords) < 2:
        continue
    trips = trips_map.get(key, 1)
    slon, slat, elon, elat = [float(x) for x in key.replace(";", ",").split(",")]
    route_records.append({
        "geometry":        LineString(coords),   # coords sind [lon, lat]
        "key":             key,
        "trips":           trips,
        "start_lon":       slon,
        "start_lat":       slat,
        "end_lon":         elon,
        "end_lat":         elat,
    })

routes_gdf = gpd.GeoDataFrame(route_records, crs="EPSG:4326")
# Als GeoJSON speichern
GEOJSON_OUT = Path("stadtradeln2023_osrm_routes.geojson")
routes_gdf.to_file(GEOJSON_OUT, driver="GeoJSON")
print(f"GeoJSON gespeichert: {GEOJSON_OUT}  ({len(routes_gdf)} Routen)")

#PolyLine-Karte
vals = [trips_map.get(k, 1) for k in cache if cache[k]]
norm = LogNorm(vmin=max(1, np.percentile(vals, 10)), vmax=np.percentile(vals, 95))
cmap = plt.get_cmap("turbo")

m = folium.Map(location=[49.487, 8.466], zoom_start=13)
for key, coords in cache.items():
    if not coords:
        continue
    t = max(trips_map.get(key, 1), norm.vmin)
    folium.PolyLine(
        [[lat, lon] for lon, lat in coords],
        color=mcolors.to_hex(cmap(norm(t))),
        weight=1 + 6 * norm(t),
        opacity=0.75,
    ).add_to(m)

m.save("stadtradeln2023_osrm_heatmap_new.html")
print("Gespeichert: stadtradeln_osrm_heatmap_new.html")
