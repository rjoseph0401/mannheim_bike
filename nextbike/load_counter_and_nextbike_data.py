"""Datenladefunktionen für Counter und Nextbike-Matching."""

from __future__ import annotations

import json
import re
from io import BytesIO
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen

import geopandas as gpd
import networkx as nx
import osmnx as ox
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
OVERVIEW_FILE = DATA_DIR.parent / "Uebersicht_Dauerahrradzaehler_201403-202510_unvollstaendig.xlsx"
GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"
CACHE_FILE = DATA_DIR / "cache" / "nextbike_edge_hits_cache.json"

NEXTBIKE_START = pd.Timestamp("2025-03-06")
NEXTBIKE_END = pd.Timestamp("2025-10-06")
GITHUB_BASE = "https://raw.githubusercontent.com/ADFC-Mannheim/q2dataride26-challenge-05-modellierung/main"

COUNTER_FILES = {
    100013246: ["Renzstraße_Ost.xlsx", "Renzstraße_West.xlsx"],
    100026599: ["Jungbuschbrücke.xlsx"],
    100042618: ["Lindenhofüberführung.xlsx"],
    100042619: ["Neckarauer_Übergang.xlsx"],
    100042620: ["Schlosspark_Lindenhof.xlsx"],
    100043759: ["Konrad_Adenauer_Brücke_Nord.xlsx", "Konrad_Adenauer_Brücke_Süd.xlsx"],
    100043761: ["Kurpfalzbrücke_Innenstadt.xlsx", "Kurpfalzbrücke_Neckarstadt.xlsx"],
    300033853: ["Feudenheimer_Straße_stadtauswärts.xlsx"],
    300033855: ["Feudenheimer_Straße_stadteinwärts.xlsx"],
    300034898: ["Luzenbergstraße.xlsx"],
    300034899: ["B 38 RI Aus.xlsx"],
    300034900: ["Theodor_Heuss_Anlage_Richtung_AUS.xlsx"],
    300034901: ["Theodor_Heuss_Anlage_Richtung_IN.xlsx"],
    300034976: ["Fernmeldeturm.xlsx"],
}

COMBINED_STATIONS = {
    "Feudenheimerstr. gesamt": [300033853, 300033855],
    "Theodor-Heuss-Anlage gesamt": [300034900, 300034901],
}

STATION_RADII_M = {
    100013246: 20,
    100042618: 15,
    100042619: 15,
    300034899: 15,
    300034900: 15,
    300034901: 15,
}


def load_edge_hits() -> dict[tuple[int, int, int], int]:
    """Lade Nextbike-Edge-Hits aus JSON Cache."""
    cache_data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    return {(u, v, k): int(count) for u, v, k, count in cache_data.get("hits", [])}


def load_excel_from_github(filename: str) -> pd.DataFrame | None:
    """Lade Counter-Excel-Datei von GitHub."""
    try:
        with urlopen(f"{GITHUB_BASE}/{quote(filename, safe='')}") as response:
            return pd.read_excel(BytesIO(response.read()), skiprows=3)
    except Exception as exc:
        print(f"  ⚠️  Fehler beim Laden von {filename}: {exc}")
        return None


def parse_datetime_column(df: pd.DataFrame) -> pd.DataFrame | None:
    """Parse erste Spalte als Datetime und entferne NaT-Zeilen."""
    first_col = df.columns[0]
    df[first_col] = pd.to_datetime(df[first_col], errors="coerce")
    df = df.dropna(subset=[first_col])
    return None if df.empty else df


def sum_counter_in_range(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> int:
    """Summiere Counter-Zahlungen im Datumsbereich."""
    filtered = df.loc[(df[df.columns[0]] >= start) & (df[df.columns[0]] <= end)]
    if filtered.empty:
        return 0

    numeric_cols = list(filtered.select_dtypes(include=["number"]).columns)
    if not numeric_cols:
        return 0

    in_cols = [c for c in numeric_cols if re.search(r"\bIN\b|\bin\b| in$|\.in$|_in$", c, flags=re.I)]
    out_cols = [c for c in numeric_cols if re.search(r"\bOUT\b|\bout\b| out$|\.out$|_out$", c, flags=re.I)]
    totals = [c for c in numeric_cols if c not in in_cols + out_cols]

    if totals:
        inout_sum = 0
        if in_cols and out_cols:
            inout_sum = filtered[in_cols].sum().sum() + filtered[out_cols].sum().sum()
        for col in totals:
            col_sum = filtered[col].sum()
            if inout_sum > 0 and abs(col_sum - inout_sum) / max(1, inout_sum) < 0.02:
                return int(col_sum)
        best = max(totals, key=lambda col: filtered[col].sum())
        return int(filtered[best].sum())

    if in_cols and out_cols:
        return int(filtered[in_cols].sum().sum() + filtered[out_cols].sum().sum())

    return int(filtered[numeric_cols].sum().sum())


def project_station_points(overview: pd.DataFrame) -> gpd.GeoDataFrame:
    """Projiziere Stationspunkte zu GeoDataFrame."""
    return gpd.GeoDataFrame(
        overview[["counter_site_id", "counter_site", "latitude", "longitude"]],
        geometry=gpd.points_from_xy(overview.longitude, overview.latitude),
        crs="EPSG:4326",
    )


def corridor_total(corridor: set[tuple[int, int, int]], edge_hits: dict[tuple[int, int, int], int]) -> int:
    """Berechne Gesamttraffic eines Korridors (Gegenrichtungen dedupliziert)."""
    undirected_best: dict[tuple[int, int, int], int] = {}
    for u, v, key in corridor:
        pair = (min(u, v), max(u, v), key)
        undirected_best[pair] = max(undirected_best.get(pair, 0), int(edge_hits.get((u, v, key), 0)))
    return sum(undirected_best.values())


def choose_corridor_component(
    graph,
    edges: gpd.GeoDataFrame,
    point,
    radius_m: int,
    edge_hits: dict[tuple[int, int, int], int],
):
    """Wähle kleinste positive zusammenhängende Komponente als Korridor."""
    nearby = edges[edges.geometry.distance(point) <= radius_m].copy()
    if nearby.empty:
        nearest_edge = ox.distance.nearest_edges(graph, X=[point.x], Y=[point.y])[0]
        nearest_row = edges.loc[edges["edge"] == nearest_edge].iloc[0]
        return {nearest_edge}, int(edge_hits.get(nearest_edge, 0)), float(nearest_row.geometry.distance(point)), nearest_row.get("name")

    nearby["dist"] = nearby.geometry.distance(point)
    H = graph.edge_subgraph(list(nearby["edge"])).copy()
    best_corridor = None
    best_score = None
    nearest_fallback = nearby.nsmallest(1, "dist").iloc[0]

    for comp in nx.weakly_connected_components(H):
        comp_edges = nearby[nearby.apply(lambda row: row.u in comp and row.v in comp, axis=1)].copy()
        if comp_edges.empty:
            continue
        corridor = set(comp_edges["edge"])
        total = corridor_total(corridor, edge_hits)
        if total <= 0:
            continue
        min_dist = float(comp_edges["dist"].min())
        score = (total, min_dist, len(corridor))
        if best_score is None or score < best_score:
            best_corridor = (corridor, total, min_dist, comp_edges.nsmallest(1, "dist").iloc[0].get("name"))
            best_score = score

    if best_corridor is not None:
        return best_corridor

    return {nearest_fallback.edge}, 0, float(nearest_fallback.dist), nearest_fallback.get("name")


def load_all_station_data() -> dict:
    """
    Lade alle Stationsdaten:
    - Stationspunkte und Koordinaten
    - Mannheim-Graph und Kanten
    - Edge-Hit-Cache
    Rückgabe: Dict mit {overview, stations, graph, edges, edge_hits}
    """
    print("🔄 Lade Overview-Datei...")
    overview = pd.read_excel(OVERVIEW_FILE)
    stations = project_station_points(overview)
    print(f"✅ {len(stations)} Stationen geladen")

    print("🔄 Lade Mannheim-Graph...")
    graph = ox.project_graph(ox.load_graphml(GRAPH_FILE))
    edges = ox.graph_to_gdfs(graph, nodes=False, edges=True).reset_index()[["u", "v", "key", "name", "geometry"]]
    edges["edge"] = list(zip(edges.u, edges.v, edges.key))
    print(f"✅ Graph mit {len(edges)} Kanten geladen")

    print("🔄 Lade Edge-Hits-Cache...")
    edge_hits = load_edge_hits()
    print(f"✅ Edge-Hits-Cache geladen ({len(edge_hits)} Edges mit Hits)")

    return {"overview": overview, "stations": stations, "graph": graph, "edges": edges, "edge_hits": edge_hits}


def compute_static_quotients(nextbike_totals: list[int], counter_totals: list[int]) -> tuple[float, float]:
    """
    Berechne zwei statische Quotienten (werden später per Zählstelle berechnet):
    - Quote 1: Counter / Gesamte Nextbike (alle Stellen)
    - Quote 2: Counter / Gesamte Counter (alle Stellen)
    """
    total_nextbike = sum(nextbike_totals)
    total_counter = sum(counter_totals)
    return total_nextbike, total_counter
