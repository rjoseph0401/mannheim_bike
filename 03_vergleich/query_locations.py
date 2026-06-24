"""
Querungsanalyse an Dauerzählstellen (2022–2024)

Für jede bekannte Fahrradzählstelle in Mannheim wird eine kurze Querlinie
(Transekt) senkrecht zur Fahrtrichtung konstruiert. Anschließend wird gezählt,
wie viele der simulierten Stadtradeln-Routen (GeoPackage je Jahr) dieses
Transekt schneiden. Die ermittelte Trip-Zahl wird der realen Referenzzählung
aus der Stadtradeln-Kampagne gegenübergestellt und als Verhältnis (Ratio)
ausgedrückt. Ergebnis wird in der Konsole ausgegeben und als CSV gespeichert
(Results/fahrtenzahlen_abfrage.csv).

Ablauf:
  1. Koordinaten & Referenzwerte der Zählstellen definieren (LOCATIONS).
  2. Routendaten für alle Jahre laden und in UTM (EPSG 25832) projizieren.
  3. Straßenrichtung je Zählstelle automatisch aus den Routengeometrien
     schätzen (auto_bearing) oder manuell überschreiben.
  4. Transekt konstruieren (make_transect) und räumliche Schnittmenge mit
     dem Spatial-Index der Routen abfragen.
  5. Trips summieren, Ratio berechnen, DataFrame ausgeben & speichern.
"""
import math
import os
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString

TRANSECT_HALF_W = 30   # halbe Breite der Querlinie in Metern
YEARS = [2022, 2023, 2024]

# Stadtradeln-Kampagnenzeiträume je Jahr (für Referenzwerte aus Dauerzählstellen)
# 2022: 20.06.–10.07.2022 | 2023: 07.05.–28.05.2023 | 2024: 03.06.–23.06.2024

# name, lat, lon, {year: ref}, road_bearing_deg
# ref=None: Zähler in dem Jahr noch nicht installiert → keine Ratio
# Feudenheimstraße und Theodor-Heuss-Anlage: Richtungen addiert
LOCATIONS = [
    ("Renzstraße",             49.49027,    8.481114,
        {2022: 102647, 2023: 103815, 2024:  95741}, None),
    ("Jungbuschbrücke",        49.49716706, 8.4591966,
        {2022:  32517, 2023:  29739, 2024:  30795}, None),
    ("Lindenhofüberführung",   49.4799213,  8.46539839,
        {2022:  37379, 2023:  21817, 2024:  43023}, None),
    ("Neckarauer Übergang",    49.47192734, 8.48418117,
        {2022:  30941, 2023:  29536, 2024:  29002}, None),
    ("Schlosspark Lindenhof",  49.48163244, 8.46035466,
        {2022:  41297, 2023:  35906, 2024:  37243}, None),
    ("Konrad-Adenauer-Brücke", 49.48159001, 8.45751554,
        {2022:  56673, 2023:  56148, 2024:  58871}, None),
    ("Kurpfalzbrücke",         49.49409911, 8.47216251,
        {2022: 139954, 2023: 148839, 2024: 115641}, None),
    ("Feudenheimstraße",       49.488887,   8.513460,       # stadtauswärts + stadteinwärts
        {2022:   None, 2023:  31706, 2024:  28096}, None),
    ("Luzenbergstr.",          49.5143795,  8.47309392,
        {2022:   None, 2023:  18999, 2024:  18703}, None),
    ("B38",                    49.5029295,  8.4976733,      # RI.AUS + RI.IN; B38 
        {2022:   None, 2023:  17871, 2024:  32496},    0),
    ("Theodor-Heuss-Anlage",   49.477773,   8.503800,       # RI.IN + RI.AUS
        {2022:   None, 2023:  25414, 2024:  20304}, None),
    ("Fernmeldeturm",          49.48678,    8.49342,
        {2022:   None, 2023:  59452, 2024:  51963}, None),
]


def make_transect(pt, road_bearing_deg, half_w=TRANSECT_HALF_W):
    """Querlinie senkrecht zur Straßenrichtung (in projiziertem CRS)."""
    perp = math.radians(road_bearing_deg + 90)
    dx = math.sin(perp) * half_w
    dy = math.cos(perp) * half_w
    return LineString([(pt.x - dx, pt.y - dy), (pt.x + dx, pt.y + dy)])


def auto_bearing(pt, gdf_utm, radius=60):
    """
    Dominante Straßenrichtung aus Routensegmenten im Umkreis schätzen.

    Alle Routen im Umkreis von `radius` Metern werden ausgeschnitten und ihre
    Richtung als Winkel in [0°, 180°) gemessen (Hin- und Rückrichtung werden
    gleichgesetzt). Der Mittelwert wird per Kreisstatistik (Trick mit doppeltem
    Winkel: b → 2b, Sinus/Kosinus summieren, atan2, zurückteilen) berechnet,
    um das Wrap-around-Problem bei Winkeln nahe 0°/180° zu vermeiden.
    Gibt 0 zurück, wenn keine Routen im Umkreis gefunden werden.
    """
    buf = pt.buffer(radius)
    hits = gdf_utm.sindex.query(buf, predicate="intersects")
    if not len(hits):
        return 0
    bearings = []
    for geom in gdf_utm.iloc[hits].geometry:
        seg = geom.intersection(buf)
        if seg.is_empty:
            continue
        parts = seg.geoms if seg.geom_type.startswith("Multi") else [seg]
        for part in parts:
            if part.geom_type != "LineString":
                continue
            coords = list(part.coords)
            if len(coords) < 2:
                continue
            dx = coords[-1][0] - coords[0][0]
            dy = coords[-1][1] - coords[0][1]
            bearings.append(math.degrees(math.atan2(dx, dy)) % 180)
    if not bearings:
        return 0
    rad2 = [math.radians(b * 2) for b in bearings]
    sx = sum(math.sin(r) for r in rad2)
    cx = sum(math.cos(r) for r in rad2)
    return math.degrees(math.atan2(sx, cx)) / 2 % 180


# Alle verfügbaren Jahre vorladen
gdfs = {}
for _year in YEARS:
    _path = f"Data/outputs/stadtradeln_graphhopper_routes_{_year}.gpkg"
    if os.path.exists(_path):
        gdfs[_year] = gpd.read_file(_path).to_crs(epsg=25832)

_gdf_ref = gdfs[min(gdfs)]

pts = gpd.GeoDataFrame(
    [(loc[0], loc[1], loc[2]) for loc in LOCATIONS],
    columns=["name", "lat", "lon"],
    geometry=[Point(loc[2], loc[1]) for loc in LOCATIONS],
    crs=4326,
).to_crs(epsg=25832)

rows = []
for loc, pt in zip(LOCATIONS, pts.geometry):
    name, lat, lon, refs, bearing_override = loc
    bearing = bearing_override if bearing_override is not None else auto_bearing(pt, _gdf_ref)
    transect = make_transect(pt, bearing)

    row = {"name": name, "lat": lat, "lon": lon, "bearing": round(bearing, 1)}
    for year, gdf in gdfs.items():
        hits  = gdf.sindex.query(transect, predicate="intersects")
        trips = int(gdf.iloc[hits]["trips"].sum())
        ref   = refs.get(year)
        row[f"trips_{year}"] = trips
        row[f"ref_{year}"]   = ref
        row[f"ratio_{year}"] = round(trips / ref, 4) if ref else None

    rows.append(row)

df = pd.DataFrame(rows)
print(df.to_string(index=False))
df.to_csv("Results/fahrtenzahlen_abfrage.csv", index=False)
