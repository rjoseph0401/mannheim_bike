from __future__ import annotations

import ast
import colorsys
from pathlib import Path

import folium
import geopandas as gpd
import pandas as pd
import osmnx as ox

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
OVERVIEW_FILE = DATA_DIR.parent / "Uebersicht_Dauerahrradzaehler_201403-202510_unvollstaendig.xlsx"
CORRIDOR_FILE = DATA_DIR / "uebersicht_dauerradzaehler_korridore.csv"
GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"
OUT_FILE = DATA_DIR / "dauerradzaehler_korridore_map.html"


def parse_station_ids(value: object) -> list[int]:
    text = str(value)
    return [int(part) for part in text.split("+") if part.strip()]


def parse_corridor_edges(value: object) -> set[tuple[int, int, int]]:
    corridor: set[tuple[int, int, int]] = set()
    if not isinstance(value, str) or not value.strip():
        return corridor

    for part in value.split("|"):
        part = part.strip()
        if not part:
            continue
        try:
            edge = ast.literal_eval(part)
        except Exception:
            continue
        if isinstance(edge, tuple) and len(edge) == 3:
            corridor.add(edge)
    return corridor


def color_for_index(index: int, total: int) -> str:
    hue = (index / max(total, 1)) % 1.0
    r, g, b = colorsys.hsv_to_rgb(hue, 0.72, 0.92)
    return "#%02x%02x%02x" % (int(r * 255), int(g * 255), int(b * 255))


def station_label(row: pd.Series) -> str:
    if "+" in str(row["counter_site_id"]):
        return str(row["counter_site"])
    return f"{row['counter_site']}"


def edge_label(edge: tuple[int, int, int]) -> str:
    return f"{edge[0]} → {edge[1]} (key {edge[2]})"


def fmt_value(value: object, digits: int = 0) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "n/a"
    if digits == 0:
        return f"{int(round(float(value))):,}".replace(",", ".")
    return f"{float(value):.{digits}f}"


def build_marker_popup(row: pd.Series) -> str:
    quotient = row.get("quotient")
    quotient_text = "n/a" if pd.isna(quotient) else f"{float(quotient) * 100:.2f}%"
    counter_total = row.get("counter_total")
    nextbike_total = row.get("nextbike_total")
    corridor_edges = row.get("corridor_edge_count")
    return (
        f"<b>{station_label(row)}</b><br>"
        f"Nextbike: {fmt_value(nextbike_total)}<br>"
        f"Zähler: {fmt_value(counter_total)}<br>"
        f"Quotient: {quotient_text}<br>"
        f"Korridor-Kanten: {fmt_value(corridor_edges)}"
    )


def mean_point(points: gpd.GeoDataFrame) -> tuple[float, float]:
    return float(points.geometry.y.mean()), float(points.geometry.x.mean())


def main() -> None:
    if not CORRIDOR_FILE.exists():
        raise FileNotFoundError(f"Fehlt: {CORRIDOR_FILE}")

    overview = pd.read_excel(OVERVIEW_FILE)
    corridor_df = pd.read_csv(CORRIDOR_FILE)

    G = ox.project_graph(ox.load_graphml(GRAPH_FILE))
    edges = ox.graph_to_gdfs(G, nodes=False, edges=True).reset_index()[["u", "v", "key", "geometry"]]
    edges["edge"] = list(zip(edges.u, edges.v, edges.key))
    edge_by_id = {edge: geom for edge, geom in zip(edges["edge"], edges["geometry"])}

    overview_points = gpd.GeoDataFrame(
        overview[["counter_site_id", "counter_site", "latitude", "longitude"]],
        geometry=gpd.points_from_xy(overview.longitude, overview.latitude),
        crs="EPSG:4326",
    )
    overview_points = overview_points.set_index("counter_site_id")

    combined_rows = corridor_df[corridor_df["counter_site_id"].astype(str).str.contains("\+")].copy()
    combined_ids = {sid for value in combined_rows["counter_site_id"] for sid in parse_station_ids(value)}

    display_rows = []
    for row in corridor_df.itertuples(index=False):
        station_ids = parse_station_ids(row.counter_site_id)
        is_combined_row = len(station_ids) > 1
        if not is_combined_row and int(row.counter_site_id) in combined_ids:
            continue

        row_dict = row._asdict()
        row_dict["station_ids"] = station_ids
        row_dict["corridor_edges_set"] = parse_corridor_edges(row.corridor_edges)
        display_rows.append(row_dict)

    display_rows.sort(key=lambda item: float(item["counter_site_id"].split("+")[0]) if "+" in str(item["counter_site_id"]) else float(item["counter_site_id"]))

    m = folium.Map(location=[49.487, 8.466], zoom_start=13, tiles="CartoDB positron")

    if display_rows:
        station_points = []
        for item in display_rows:
            station_ids = item["station_ids"]
            points = overview_points.loc[overview_points.index.isin(station_ids)].copy()
            if points.empty:
                continue
            station_points.append(mean_point(points))

        if station_points:
            latitudes = [lat for lat, _ in station_points]
            longitudes = [lon for _, lon in station_points]
            m.fit_bounds([[min(latitudes), min(longitudes)], [max(latitudes), max(longitudes)]])

    for index, item in enumerate(display_rows):
        color = color_for_index(index, len(display_rows))
        station_ids = item["station_ids"]
        points = overview_points.loc[overview_points.index.isin(station_ids)].copy()
        if points.empty:
            continue

        center_lat, center_lon = mean_point(points)
        popup_html = build_marker_popup(pd.Series(item))

        feature_group = folium.FeatureGroup(name=station_label(pd.Series(item)), show=True)

        corridor_edges = item["corridor_edges_set"]
        for edge in corridor_edges:
            geom = edge_by_id.get(edge)
            if geom is None:
                continue
            coordinates = [[lat, lon] for lon, lat in geom.coords]
            folium.PolyLine(
                coordinates,
                color="#111111",
                weight=9,
                opacity=0.25,
                tooltip=f"{station_label(pd.Series(item))}: {edge_label(edge)}",
            ).add_to(feature_group)
            folium.PolyLine(
                coordinates,
                color=color,
                weight=6,
                opacity=0.95,
                tooltip=f"{station_label(pd.Series(item))}: {edge_label(edge)}",
            ).add_to(feature_group)

        folium.CircleMarker(
            location=[center_lat, center_lon],
            radius=7,
            color=color,
            weight=3,
            fill=True,
            fill_color=color,
            fill_opacity=1.0,
            tooltip=station_label(pd.Series(item)),
            popup=folium.Popup(popup_html, max_width=320),
        ).add_to(feature_group)

        for _, point_row in points.iterrows():
            folium.CircleMarker(
                location=[point_row.geometry.y, point_row.geometry.x],
                radius=4,
                color="#222222",
                weight=1,
                fill=True,
                fill_color="#ffffff",
                fill_opacity=0.9,
                tooltip=point_row["counter_site"],
            ).add_to(feature_group)

        feature_group.add_to(m)

    legend_items = []
    for index, item in enumerate(display_rows):
        color = color_for_index(index, len(display_rows))
        legend_items.append(
            f"<div style='display:flex;align-items:center;margin:3px 0;'>"
            f"<div style='width:14px;height:14px;background:{color};margin-right:8px;border:1px solid #333;'></div>"
            f"<div style='font-size:12px'>{station_label(pd.Series(item))}</div>"
            f"</div>"
        )

    legend_html = """
    <div style="
        position: fixed;
        bottom: 24px;
        left: 24px;
        z-index: 9999;
        background: rgba(255,255,255,0.95);
        border: 1px solid #bbb;
        border-radius: 10px;
        padding: 12px 14px;
        max-height: 40vh;
        overflow-y: auto;
        box-shadow: 0 2px 10px rgba(0,0,0,0.18);
        min-width: 260px;
    ">
            <div style="font-weight:700;margin-bottom:8px;">Zähler-Edges</div>
            <div style="font-size:12px;margin-bottom:8px;">Jede Farbe steht für die Edges eines Zählers</div>
      {items}
    </div>
    """.format(items="".join(legend_items))
    m.get_root().html.add_child(folium.Element(legend_html))
    folium.LayerControl(collapsed=False).add_to(m)
    m.save(OUT_FILE)
    print(f"Gespeichert: {OUT_FILE}")


if __name__ == "__main__":
    main()
