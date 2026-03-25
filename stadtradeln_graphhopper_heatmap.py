from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import json
import time
import math

import numpy as np
import geopandas as gpd
import pandas as pd
import requests
import folium
import branca.colormap as bcm
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.colors import LogNorm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from shapely.geometry import LineString, Point

# --------------------------------------------------
# Basisverzeichnisse
# --------------------------------------------------
try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path.cwd()

DATA_DIR = BASE_DIR / "Data"
CACHE_DIR = DATA_DIR / "cache"
OUT_DIR = DATA_DIR / "outputs"

# Ordner sicherstellen
CACHE_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# Input
# --------------------------------------------------
INPUT_FILE = DATA_DIR / "stadtradeln_2024.xlsx"

# --------------------------------------------------
# Cache
# --------------------------------------------------
PARQUET = CACHE_DIR / "stadtradeln_2024.parquet"
CACHE_FILE = CACHE_DIR / "stadtradeln_graphhopper_routes.json"

# --------------------------------------------------
# Outputs
# --------------------------------------------------
OUT_GPKG = OUT_DIR / "stadtradeln_graphhopper_routes.gpkg"
OUT_FAILED_GPKG = OUT_DIR / "stadtradeln_graphhopper_failed_pairs.gpkg"
OUT_HTML = OUT_DIR / "stadtradeln_graphhopper_heatmap.html"

GH_LOCAL = "http://localhost:8989"
PROFILE = "bike"

MAX_WORKERS = 16
MAX_IN_FLIGHT = 200
SUBMIT_PROGRESS_EVERY = 1000
DONE_PROGRESS_EVERY = 50
CHECKPOINT_EVERY = 500
REQUEST_TIMEOUT = 30

WRITE_HTML_PREVIEW = True
HTML_TOP_N = 20000
SIMPLIFY_TOLERANCE = 0.0

RECOMPUTE_STATUSES = {
    "legacy_failed",
    "no_path",
    "timeout",
    "exception",
    "unknown_cache_format",
    "http_400",
}

EXTRACT_BBOX = {
    "west": 8.177,
    "south": 49.324,
    "east": 8.825,
    "north": 49.618,
}

MAX_SHOW_400 = 20
shown_400 = 0


# ============================================================
# Hilfsfunktionen
# ============================================================

def fmt_seconds(seconds: float) -> str:
    if not np.isfinite(seconds) or seconds < 0:
        return "?"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def make_session():
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS,
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def save_cache(cache, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache), encoding="utf-8")


def load_cache(path):
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_cached_result(value):
    if isinstance(value, dict) and "status" in value and "coords" in value:
        return value

    if isinstance(value, list):
        return {
            "status": "ok",
            "coords": value,
            "attempt": "legacy",
            "detail": None,
        }

    if value is None:
        return {
            "status": "legacy_failed",
            "coords": None,
            "attempt": None,
            "detail": None,
        }

    return {
        "status": "unknown_cache_format",
        "coords": None,
        "attempt": None,
        "detail": None,
    }


def simplify_coords(coords, tolerance=0.0):
    if not coords or tolerance <= 0:
        return coords
    line = LineString(coords)
    line_simple = line.simplify(tolerance, preserve_topology=False)
    return list(line_simple.coords)


def request_route(session, slon, slat, elon, elat):
    params = [
        ("point", f"{slat},{slon}"),
        ("point", f"{elat},{elon}"),
        ("profile", PROFILE),
        ("points_encoded", "false"),
    ]

    resp = session.get(f"{GH_LOCAL}/route", params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    j = resp.json()

    if "paths" in j and j["paths"]:
        coords = j["paths"][0]["points"]["coordinates"]
        return coords, None

    return None, None


def get_route(session, slon, slat, elon, elat):
    global shown_400

    try:
        coords, detail = request_route(session, slon, slat, elon, elat)

        if coords:
            return {
                "status": "ok",
                "coords": coords,
                "attempt": "plain",
                "detail": detail,
            }

        return {
            "status": "no_path",
            "coords": None,
            "attempt": None,
            "detail": None,
        }

    except requests.Timeout:
        return {
            "status": "timeout",
            "coords": None,
            "attempt": None,
            "detail": None,
        }

    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else "unknown"

        detail = ""
        try:
            detail = e.response.text
        except Exception:
            pass

        if code == 400 and shown_400 < MAX_SHOW_400:
            print(f"\nHTTP 400 Detail #{shown_400 + 1}")
            print(f"Start: ({slon}, {slat})")
            print(f"Ende : ({elon}, {elat})")
            print(detail[:1000])
            print("-" * 80)
            shown_400 += 1

        return {
            "status": f"http_{code}",
            "coords": None,
            "attempt": None,
            "detail": detail[:1000] if detail else None,
        }

    except Exception as e:
        return {
            "status": "exception",
            "coords": None,
            "attempt": None,
            "detail": str(e)[:500],
        }


def add_html_legend(fmap, vmin, vmax, cmap_name="turbo"):
    if not np.isfinite(vmin) or not np.isfinite(vmax) or vmax <= vmin:
        return

    log_min = math.log10(vmin)
    log_max = math.log10(vmax)

    cmap = plt.get_cmap(cmap_name)
    colors = [mcolors.to_hex(cmap(x)) for x in np.linspace(0, 1, 8)]

    legend = bcm.LinearColormap(
        colors=colors,
        vmin=log_min,
        vmax=log_max,
        caption="Trips pro OD-Paar (log-Skala)",
    )

    ticks = np.linspace(log_min, log_max, 6)
    legend.index = list(ticks)
    legend.tick_labels = [str(int(round(10 ** t))) for t in ticks]

    legend.add_to(fmap)


# ============================================================
# Daten laden
# ============================================================

print("[1/7] Lade und bereinige Daten ...")
PARQUET.parent.mkdir(exist_ok=True)

if not PARQUET.exists():
    df = pd.read_excel(INPUT_FILE)
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
    )

    for c in ["x_start", "y_start", "x_end", "y_end", "number_of_matched_trips"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(
        subset=["x_start", "y_start", "x_end", "y_end", "number_of_matched_trips"]
    ).copy()

    df.to_parquet(PARQUET)
    print(f"  Parquet erstellt: {PARQUET.name}")
else:
    print(f"  Parquet gefunden: {PARQUET.name}")

df = pd.read_parquet(PARQUET)
print(f"  Zeilen im Datensatz: {len(df):,}")


# ============================================================
# CRS / OD-Paare
# ============================================================

print("[2/7] Transformiere Koordinaten und aggregiere OD-Paare ...")

pts_s = gpd.GeoDataFrame(
    geometry=gpd.points_from_xy(df.x_start, df.y_start),
    crs="EPSG:25832"
).to_crs(4326)

pts_e = gpd.GeoDataFrame(
    geometry=gpd.points_from_xy(df.x_end, df.y_end),
    crs="EPSG:25832"
).to_crs(4326)

df["slon"], df["slat"] = pts_s.geometry.x.values, pts_s.geometry.y.values
df["elon"], df["elat"] = pts_e.geometry.x.values, pts_e.geometry.y.values

pairs = (
    df.groupby(["slon", "slat", "elon", "elat"], as_index=False)["number_of_matched_trips"]
    .sum()
)

pairs = pairs[(pairs.slon != pairs.elon) | (pairs.slat != pairs.elat)].copy()

trips_map = {
    f"{r.slon},{r.slat};{r.elon},{r.elat}": int(r.number_of_matched_trips)
    for r in pairs.itertuples()
}

print(f"  Einzigartige Paare: {len(pairs):,}")


# ============================================================
# Extrakt gegen Daten prüfen
# ============================================================

print("[3/7] Prüfe Bounding Box der Daten gegen den OSM-Extrakt ...")

all_lons = pd.concat([df["slon"], df["elon"]], ignore_index=True)
all_lats = pd.concat([df["slat"], df["elat"]], ignore_index=True)

print("  Daten-BBox:")
print(f"    Lon: {all_lons.min():.6f} bis {all_lons.max():.6f}")
print(f"    Lat: {all_lats.min():.6f} bis {all_lats.max():.6f}")

outside_mask = (
    (all_lons < EXTRACT_BBOX["west"]) |
    (all_lons > EXTRACT_BBOX["east"]) |
    (all_lats < EXTRACT_BBOX["south"]) |
    (all_lats > EXTRACT_BBOX["north"])
)

outside_count = int(outside_mask.sum())

print("  Extrakt-BBox:")
print(f"    West={EXTRACT_BBOX['west']}, Ost={EXTRACT_BBOX['east']}, "
      f"Süd={EXTRACT_BBOX['south']}, Nord={EXTRACT_BBOX['north']}")
print(f"  Punkte außerhalb des Extrakts: {outside_count:,}")

if outside_count > 0:
    print("  WARNUNG: Mindestens ein Teil der Punkte liegt außerhalb des OSM-Extrakts.")


# ============================================================
# Session / Cache / Health Check
# ============================================================

print("[4/7] Initialisiere GraphHopper-Verbindung ...")

session = make_session()

cache_raw = load_cache(CACHE_FILE)
cache = {k: normalize_cached_result(v) for k, v in cache_raw.items()}

status_before = {}
for v in cache.values():
    s = normalize_cached_result(v)["status"]
    status_before[s] = status_before.get(s, 0) + 1

print(f"  Cache-Einträge: {len(cache):,}")
print(f"  Cache-Status vor Neuberechnung: {status_before}")

todo = []
todo_reason_counts = {}

for r in pairs.itertuples():
    key = f"{r.slon},{r.slat};{r.elon},{r.elat}"
    cached = cache.get(key)

    if cached is None:
        todo.append((key, r.slon, r.slat, r.elon, r.elat))
        todo_reason_counts["missing"] = todo_reason_counts.get("missing", 0) + 1
    else:
        cached = normalize_cached_result(cached)
        if cached["status"] in RECOMPUTE_STATUSES:
            todo.append((key, r.slon, r.slat, r.elon, r.elat))
            s = cached["status"]
            todo_reason_counts[s] = todo_reason_counts.get(s, 0) + 1

print(f"  Neu zu berechnen: {len(todo):,}")
print(f"  Bereits im Cache: {len(pairs) - len(todo):,}")
print(f"  Gründe für Neuberechnung: {todo_reason_counts}")

print("  Health-Check gegen GraphHopper ...")

try:
    resp = session.get(
        f"{GH_LOCAL}/route",
        params=[
            ("point", "49.487,8.466"),
            ("point", "49.490,8.470"),
            ("profile", PROFILE),
            ("points_encoded", "false"),
        ],
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    j = resp.json()

    if "paths" not in j or not j["paths"]:
        raise RuntimeError("GraphHopper antwortet, liefert aber keine Test-Route.")

    print("  Health-Check API: OK")

except Exception as e:
    raise RuntimeError(
        f"GraphHopper-Health-Check fehlgeschlagen: {e}. "
        f"Container/API prüfen: {GH_LOCAL}/route"
    )

if todo:
    test_key, test_slon, test_slat, test_elon, test_elat = todo[0]
    test_result = get_route(session, test_slon, test_slat, test_elon, test_elat)

    if test_result["coords"] is None:
        print(f"  Erstes OD-Paar nicht routbar (status={test_result['status']})")
    else:
        print(f"  Erstes OD-Paar routbar (attempt={test_result['attempt']})")
else:
    print("  Keine neuen Routen zu berechnen.")


# ============================================================
# Routing
# ============================================================

if todo:
    print("[5/7] Berechne Routen ...")
    print(
        f"  MAX_WORKERS={MAX_WORKERS}, MAX_IN_FLIGHT={MAX_IN_FLIGHT}, "
        f"TIMEOUT={REQUEST_TIMEOUT}s"
    )

    total = len(todo)
    submitted = 0
    completed = 0
    failed = 0

    status_counts = {}
    attempt_counts = {}

    t_submit0 = time.time()
    t_done0 = time.time()

    future_to_key = {}

    def submit_one(executor, item):
        key, slon, slat, elon, elat = item
        fut = executor.submit(get_route, session, slon, slat, elon, elat)
        future_to_key[fut] = key

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        todo_iter = iter(todo)

        print("  Fülle Request-Queue ...")
        while len(future_to_key) < min(MAX_IN_FLIGHT, total):
            try:
                item = next(todo_iter)
            except StopIteration:
                break

            submit_one(ex, item)
            submitted += 1

            if submitted % SUBMIT_PROGRESS_EVERY == 0 or submitted == min(MAX_IN_FLIGHT, total):
                elapsed = time.time() - t_submit0
                rate = submitted / elapsed if elapsed > 0 else 0
                print(
                    f"  Queue: {submitted:,}/{total:,} eingereiht "
                    f"({rate:.1f} submits/s, in-flight={len(future_to_key):,})"
                )

        while future_to_key:
            done, _ = wait(list(future_to_key.keys()), return_when=FIRST_COMPLETED)

            for fut in done:
                key = future_to_key.pop(fut)
                result = fut.result()
                cache[key] = result

                completed += 1

                status = result["status"]
                attempt = result.get("attempt")

                status_counts[status] = status_counts.get(status, 0) + 1
                if attempt:
                    attempt_counts[attempt] = attempt_counts.get(attempt, 0) + 1

                if result["coords"] is None:
                    failed += 1

                while len(future_to_key) < MAX_IN_FLIGHT:
                    try:
                        item = next(todo_iter)
                    except StopIteration:
                        break

                    submit_one(ex, item)
                    submitted += 1

                    if submitted % SUBMIT_PROGRESS_EVERY == 0 or submitted == total:
                        elapsed_submit = time.time() - t_submit0
                        rate_submit = submitted / elapsed_submit if elapsed_submit > 0 else 0
                        print(
                            f"  Queue: {submitted:,}/{total:,} eingereiht "
                            f"({rate_submit:.1f} submits/s, in-flight={len(future_to_key):,})"
                        )

                if completed % DONE_PROGRESS_EVERY == 0 or completed == total:
                    elapsed = time.time() - t_done0
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = total - completed
                    eta = remaining / rate if rate > 0 else float("inf")

                    print(
                        f"  Fertig: {completed:,}/{total:,} | "
                        f"failed={failed:,} | "
                        f"rate={rate:.2f} routes/s | "
                        f"elapsed={fmt_seconds(elapsed)} | "
                        f"ETA={fmt_seconds(eta)} | "
                        f"in-flight={len(future_to_key):,}"
                    )

                if completed % CHECKPOINT_EVERY == 0 or completed == total:
                    save_cache(cache, CACHE_FILE)
                    print(f"  Checkpoint gespeichert: {CACHE_FILE.name} ({len(cache):,} Einträge)")

    print("  Routing abgeschlossen.")
    print(f"  Statusverteilung neuer Lauf: {status_counts}")
    print(f"  Attempt-Verteilung neuer Lauf: {attempt_counts}")

else:
    save_cache(cache, CACHE_FILE)


# ============================================================
# Geo-Objekte aus Cache aufbauen
# ============================================================

print("  Bereite Geo-Daten für Export vor ...")

route_rows = []
failed_rows = []

for r in pairs.itertuples():
    key = f"{r.slon},{r.slat};{r.elon},{r.elat}"
    result = normalize_cached_result(cache.get(key))

    status = result["status"]
    coords = result.get("coords")
    attempt = result.get("attempt")
    detail = result.get("detail")
    trips = int(r.number_of_matched_trips)

    if coords:
        coords = simplify_coords(coords, SIMPLIFY_TOLERANCE)

        if coords and len(coords) >= 2:
            route_rows.append(
                {
                    "key": key,
                    "slon": float(r.slon),
                    "slat": float(r.slat),
                    "elon": float(r.elon),
                    "elat": float(r.elat),
                    "trips": trips,
                    "status": status,
                    "attempt": attempt,
                    "detail": detail,
                    "geometry": LineString(coords),
                }
            )
        else:
            failed_rows.append(
                {
                    "key": key,
                    "slon": float(r.slon),
                    "slat": float(r.slat),
                    "elon": float(r.elon),
                    "elat": float(r.elat),
                    "trips": trips,
                    "status": "invalid_geometry",
                    "attempt": attempt,
                    "detail": "Route returned fewer than 2 coordinates after simplification.",
                    "geometry": Point(float(r.slon), float(r.slat)),
                }
            )
    else:
        failed_rows.append(
            {
                "key": key,
                "slon": float(r.slon),
                "slat": float(r.slat),
                "elon": float(r.elon),
                "elat": float(r.elat),
                "trips": trips,
                "status": status,
                "attempt": attempt,
                "detail": detail,
                "geometry": Point(float(r.slon), float(r.slat)),
            }
        )

print(f"  Erfolgreiche Routen für Export: {len(route_rows):,}")
print(f"  Fehlgeschlagene Paare für Export: {len(failed_rows):,}")


# -------------------------------------------------------
# [6/7] Schreibe GeoPackage für QGIS
# -------------------------------------------------------

print("[6/7] Schreibe GeoPackage für QGIS ...")

if route_rows:
    gdf_routes = gpd.GeoDataFrame(
        route_rows,
        geometry="geometry",
        crs="EPSG:4326"
    )

    if OUT_GPKG.exists():
        OUT_GPKG.unlink()

    gdf_routes.to_file(
        OUT_GPKG,
        layer="routes",
        driver="GPKG"
    )

    print(f"  Gespeichert: {OUT_GPKG.name}")
    print(f"  Linien im GeoPackage: {len(gdf_routes):,}")
else:
    gdf_routes = gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")
    print("  Keine erfolgreichen Routen zum Speichern.")

if failed_rows:
    gdf_failed = gpd.GeoDataFrame(
        failed_rows,
        geometry="geometry",
        crs="EPSG:4326"
    )

    if OUT_FAILED_GPKG.exists():
        OUT_FAILED_GPKG.unlink()

    gdf_failed.to_file(
        OUT_FAILED_GPKG,
        layer="failed_pairs",
        driver="GPKG"
    )

    print(f"  Fehlgeschlagene Paare gespeichert: {OUT_FAILED_GPKG.name}")
    print(f"  Fehlgeschlagene Features: {len(gdf_failed):,}")

    if len(gdf_routes) > 0:
        gdf_failed.to_file(
            OUT_GPKG,
            layer="failed_routes",
            driver="GPKG"
        )
        print("  Layer 'failed_routes' zusätzlich in Haupt-GPKG gespeichert.")
else:
    print("  Keine fehlgeschlagenen Routen vorhanden.")


print("[7/7] Erzeuge optionale HTML-Vorschau ...")

if WRITE_HTML_PREVIEW and len(gdf_routes) > 0:
    gdf_preview = gdf_routes.sort_values("trips", ascending=False).head(HTML_TOP_N).copy()

    vals = gdf_preview["trips"].astype(float).tolist()
    vmin = max(1.0, float(np.percentile(vals, 10)))
    vmax = float(np.percentile(vals, 95))

    if vmax <= vmin:
        vmax = max(vmin + 1.0, max(vals))

    norm = LogNorm(vmin=vmin, vmax=vmax)
    cmap = plt.get_cmap("turbo")

    m = folium.Map(location=[49.487, 8.466], zoom_start=13, tiles="OpenStreetMap")

    for i, row in enumerate(gdf_preview.itertuples(), 1):
        coords = list(row.geometry.coords)
        t = max(float(row.trips), vmin)

        folium.PolyLine(
            [[lat, lon] for lon, lat in coords],
            color=mcolors.to_hex(cmap(norm(t))),
            weight=1 + 6 * norm(t),
            opacity=0.75,
            popup=(
                f"Trips: {row.trips}<br>"
                f"Status: {row.status}<br>"
                f"Attempt: {row.attempt}"
            ),
        ).add_to(m)

        if i % 5000 == 0:
            print(f"  {i:,} Linien zur HTML-Vorschau hinzugefügt ...")

    add_html_legend(m, vmin=vmin, vmax=vmax, cmap_name="turbo")

    # Zusätzliche Tick-Werte auf der bestehenden Legende setzen
    log_min = math.log10(vmin)
    log_max = math.log10(vmax)

    ticks = np.linspace(log_min, log_max, 6)
    tick_labels = []

    for t in ticks:
        value = 10 ** t
        if value >= 100:
            tick_labels.append(f"{int(round(value))}")
        elif value >= 10:
            tick_labels.append(f"{value:.1f}")
        else:
            tick_labels.append(f"{value:.2f}")

    for child in m._children.values():
        if isinstance(child, bcm.LinearColormap):
            child.index = list(ticks)
            child.tick_labels = tick_labels
            break

    m.save(OUT_HTML)
    print(f"  Gespeichert: {OUT_HTML.name}")
else:
    print("  HTML-Vorschau übersprungen.")