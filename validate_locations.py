import geopandas as gpd
import folium
from shapely.geometry import Point
from query_locations import LOCATIONS, TRANSECT_HALF_W, gdfs, _gdf_ref, make_transect, auto_bearing

DISPLAY_RADIUS = 150

gdf_utm = _gdf_ref

pts = gpd.GeoDataFrame(
    [(loc[0], loc[1], loc[2]) for loc in LOCATIONS],
    columns=["name", "lat", "lon"],
    geometry=[Point(loc[2], loc[1]) for loc in LOCATIONS],
    crs=4326,
).to_crs(epsg=25832)

def marker_color(max_r):
    if max_r is None:   return "gray"
    if max_r > 0.20:    return "red"
    if max_r > 0.10:    return "orange"
    return "green"

m = folium.Map(location=[49.487, 8.476], zoom_start=13, tiles="CartoDB positron")
sindex = gdf_utm.sindex

for loc, pt in zip(LOCATIONS, pts.geometry):
    name, lat, lon, refs, bearing_override = loc
    bearing = bearing_override if bearing_override is not None else auto_bearing(pt, gdf_utm)
    transect = make_transect(pt, bearing)
    buf_display = pt.buffer(DISPLAY_RADIUS)

    hits_counted  = set(sindex.query(transect,    predicate="intersects"))
    hits_local    = set(sindex.query(buf_display, predicate="intersects"))
    hits_excluded = hits_local - hits_counted

    def draw(indices, color):
        if not indices:
            return
        clipped = gdf_utm.iloc[list(indices)].geometry.intersection(buf_display)
        for geom in gpd.GeoSeries(clipped, crs=25832).to_crs(4326):
            if not geom.is_empty:
                folium.GeoJson(
                    geom.__geo_interface__,
                    style_function=lambda x, c=color: {"color": c, "weight": 2, "opacity": 0.75},
                ).add_to(m)

    draw(hits_counted,  "#e74c3c")
    draw(hits_excluded, "#f39c12")

    # Ratios berechnen
    ratio_lines = []
    max_r = None
    for year, gdf in gdfs.items():
        ref = refs.get(year)
        hits = gdf.sindex.query(transect, predicate="intersects")
        trips = int(gdf.iloc[hits]["trips"].sum())
        if ref:
            r = trips / ref
            max_r = max(max_r, r) if max_r is not None else r
            ratio_lines.append(f"{year}: {trips} / {ref} = <b>{r:.1%}</b>")
        else:
            ratio_lines.append(f"{year}: {trips} trips (kein Ref)")

    popup_html = (
        f"<b>{name}</b><br>"
        f"bearing={bearing:.1f}°<br>"
        + "<br>".join(ratio_lines)
    )

    t_wgs = gpd.GeoSeries([transect], crs=25832).to_crs(4326).iloc[0]
    folium.GeoJson(
        t_wgs.__geo_interface__,
        style_function=lambda x: {"color": "#2980b9", "weight": 4, "opacity": 0.9},
    ).add_to(m)

    folium.Marker(
        location=[lat, lon],
        tooltip=f"{name} | max={max_r:.1%}" if max_r is not None else name,
        popup=folium.Popup(popup_html, max_width=260),
        icon=folium.Icon(color=marker_color(max_r), icon="info-sign"),
    ).add_to(m)

out = "Results/validate_locations.html"
m.save(out)
print(f"Gespeichert: {out}")
