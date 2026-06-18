from pathlib import Path
import ast

import folium
from folium.plugins import DualMap
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import pandas as pd
from matplotlib.colors import BoundaryNorm

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
INPUT_FILE = DATA_DIR / "df_nextbike_merged_mit_routen.csv"
PERIOD_WINDOWS = [
    ("05-01", "06-01", "Mai-June"),
    ("07-25", "08-25", "Jul-Aug"),
]


def parse_route(value):
    if not isinstance(value, str) or not value.strip().startswith("["):
        return None
    try:
        coords = ast.literal_eval(value)
        return coords if isinstance(coords, list) and len(coords) > 1 else None
    except Exception:
        return None


def period_pairs(df, start, end):
    p = df[(df["rueckgabe_datetime"] >= pd.Timestamp(start)) & (df["rueckgabe_datetime"] < pd.Timestamp(end))].copy()
    p = p.groupby(["route_als_liste"], as_index=False).size()
    p = p.rename(columns={"size": "trips"})
    p["coords"] = p["route_als_liste"].apply(parse_route)
    p = p[p["coords"].notna()].copy()
    p = p.sort_values("trips", ascending=True)
    return p


def draw_period(target_map, pairs, norm, cmap):
    for r in pairs.itertuples():
        t = max(r.trips, norm.boundaries[0])
        z = norm(t)
        folium.PolyLine(
            [[lat, lon] for lon, lat in r.coords],
            color=mcolors.to_hex(cmap(z)),
            weight=0.6 + 3.0 * ((z / max(1, cmap.N - 1)) ** 0.9),
            opacity=0.82,
            line_cap="butt",
        ).add_to(target_map)


df = pd.read_csv(
    INPUT_FILE,
    usecols=["rueckgabe_datetime", "start_lon", "start_lat", "end_lon", "end_lat", "route_als_liste"],
)
df["rueckgabe_datetime"] = pd.to_datetime(df["rueckgabe_datetime"], errors="coerce")
for c in ["start_lon", "start_lat", "end_lon", "end_lat"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["rueckgabe_datetime", "start_lon", "start_lat", "end_lon", "end_lat"]).copy()

target_year = int(df["rueckgabe_datetime"].dt.year.mode().iloc[0])
PERIODS = [
    (f"{target_year}-{s}", f"{target_year}-{e}", f"{label} {target_year}")
    for s, e, label in PERIOD_WINDOWS
]
print("Vergleichsjahr:", target_year)

left = period_pairs(df, PERIODS[0][0], PERIODS[0][1])
right = period_pairs(df, PERIODS[1][0], PERIODS[1][1])

vals = pd.concat([left["trips"], right["trips"]], ignore_index=True)
if len(vals) == 0:
    boundaries = np.array([1, 2])
    print("Warnung: Keine Fahrten in den gewählten Zeiträumen gefunden.")
    print("Datenbereich im Datensatz:", df["rueckgabe_datetime"].min(), "bis", df["rueckgabe_datetime"].max())
else:
    vmin = max(1, int(np.percentile(vals, 5)))
    vmax = int(np.percentile(vals, 99.5))
    vmax = max(vmax, vmin + 1)
    boundaries = np.unique(np.logspace(np.log10(vmin), np.log10(vmax), 8).astype(int))
    if len(boundaries) < 2:
        boundaries = np.array([vmin, vmax])
    elif boundaries[0] > vmin:
        boundaries = np.insert(boundaries, 0, vmin)
    if boundaries[-1] < vmax:
        boundaries = np.append(boundaries, vmax)
cmap = plt.get_cmap("RdYlBu_r", len(boundaries) - 1)
norm = BoundaryNorm(boundaries, cmap.N)

dm = DualMap(location=[49.487, 8.466], zoom_start=13)
draw_period(dm.m1, left, norm, cmap)
draw_period(dm.m2, right, norm, cmap)

folium.Marker(
    [49.58, 8.30],
    icon=folium.DivIcon(html=f"<div style='font-size:12px'><b>Links: {PERIODS[0][2]}</b><br><b>Rechts: {PERIODS[1][2]}</b></div>"),
).add_to(dm.m1)

out = DATA_DIR / "nextbike_compare_heatmap_2024_periods.html"
dm.save(out)
print("Gespeichert:", out)
print(f"Linien links/rechts: {len(left)} / {len(right)}")
