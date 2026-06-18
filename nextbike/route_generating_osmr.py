import os
import pandas as pd
import requests
import json
from json import JSONDecodeError
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/

# Input CSV with raw Nextbike trips. Override with env var NEXTBIKE_CSV.
NEXTBIKE_CSV = Path(os.environ.get("NEXTBIKE_CSV", DATA_DIR.parent / "download" / "touren_Nextbike.csv"))

df_nextbike = pd.read_csv(NEXTBIKE_CSV)
nb = df_nextbike.copy()
nb.columns = (nb.columns.astype(str).str.strip().str.lower()
              .str.replace(r"\s+", "_", regex=True)
              .str.replace(r"[^\w]", "", regex=True))
req = ["ausleihstationname", "rueckgabestationname", "start_lat", "start_lon", "end_lat", "end_lon"]
nb = nb.dropna(subset=req).copy()
for c in ["start_lat", "start_lon", "end_lat", "end_lon"]:
    nb[c] = pd.to_numeric(nb[c], errors="coerce").round(6)

start_xy = nb[["ausleihstationname", "start_lat", "start_lon"]].rename(
    columns={"ausleihstationname": "station", "start_lat": "lat", "start_lon": "lon"}
)
end_xy = nb[["rueckgabestationname", "end_lat", "end_lon"]].rename(
    columns={"rueckgabestationname": "station", "end_lat": "lat", "end_lon": "lon"}
)
stations = pd.concat([start_xy, end_xy], ignore_index=True).groupby("station", as_index=False)[["lat", "lon"]].mean()

pairs = nb[["ausleihstationname", "rueckgabestationname"]].drop_duplicates().rename(
    columns={"ausleihstationname": "start_station", "rueckgabestationname": "end_station"}
)
pairs = pairs.merge(stations, left_on="start_station", right_on="station", how="left").rename(
    columns={"lat": "start_lat", "lon": "start_lon"}
).drop(columns=["station"])
pairs = pairs.merge(stations, left_on="end_station", right_on="station", how="left").rename(
    columns={"lat": "end_lat", "lon": "end_lon"}
).drop(columns=["station"])
pairs = pairs.dropna(subset=["start_lat", "start_lon", "end_lat", "end_lon"]).copy()
pairs = pairs[pairs["start_station"] != pairs["end_station"]].copy()

cache_file = DATA_DIR / "route_cache_station_pairs_bike2.json"


def load_route_cache(path: Path):
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except JSONDecodeError as e:
        broken_name = path.with_suffix(path.suffix + f".corrupt_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        path.replace(broken_name)
        print(f"Warnung: Ungültige Cache-Datei erkannt ({e}).")
        print(f"Defekte Datei wurde umbenannt zu: {broken_name}")
        print("Neuer Cache wird leer gestartet.")
        return {}


def save_route_cache(path: Path, cache_dict):
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(cache_dict, f, ensure_ascii=False)
    tmp_path.replace(path)


route_cache = load_route_cache(cache_file)

def pair_key(start_station, end_station):
    return f"{start_station}|||{end_station}"

def get_route(slon, slat, elon, elat, retries=2):
    url = f"https://routing.openstreetmap.de/routed-bike/route/v1/bike/{slon},{slat};{elon},{elat}"
    for _ in range(retries + 1):
        try:
            r = requests.get(url, params={"overview": "full", "geometries": "geojson"}, timeout=(5, 20))
            r.raise_for_status()
            j = r.json()
            if j.get("code") == "Ok" and j.get("routes"):
                return j["routes"][0]["geometry"]["coordinates"]
        except Exception:
            pass
    return None

todo = []
for row in pairs.itertuples(index=False):
    key = pair_key(row.start_station, row.end_station)
    if key not in route_cache:
        todo.append((key, row.start_lon, row.start_lat, row.end_lon, row.end_lat))

max_workers = 75
done = 0
if todo:
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(get_route, slon, slat, elon, elat): key for key, slon, slat, elon, elat in todo}
        for fut in as_completed(futures):
            key = futures[fut]
            route_cache[key] = fut.result()
            done += 1
            if done % 200 == 0 or done == len(todo):
                save_route_cache(cache_file, route_cache)
                print(f"Fortschritt: {done}/{len(todo)} neue Stationspaare")

pairs["route_als_liste"] = [
    route_cache.get(pair_key(s, e), None)
    for s, e in pairs[["start_station", "end_station"]].to_numpy()
]
pairs["route_als_liste"] = pairs["route_als_liste"].apply(lambda x: pd.NA if x is None else x)

df_nextbike_merged = nb.merge(
    pairs[["start_station", "end_station", "route_als_liste"]],
    left_on=["ausleihstationname", "rueckgabestationname"],
    right_on=["start_station", "end_station"],
    how="left"
).drop(columns=["start_station", "end_station"])

output_file = DATA_DIR / "df_nextbike_merged_mit_routen.csv"
df_nextbike_merged.to_csv(output_file, index=False)

print("Rows:", len(df_nextbike_merged))
print("Routen gefunden:", df_nextbike_merged["route_als_liste"].notna().sum())
print("Stationspaare gesamt:", len(pairs))
print("Cache-Datei:", str(cache_file))
print("Gespeichert als:", output_file)
