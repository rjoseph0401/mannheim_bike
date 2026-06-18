from __future__ import annotations

import ast
import json
import re
import unicodedata
from pathlib import Path

import geopandas as gpd
import networkx as nx
import osmnx as ox
import pandas as pd
from shapely.geometry import Point

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
OVERVIEW_FILE = DATA_DIR.parent / "Uebersicht_Dauerahrradzaehler_201403-202510_unvollstaendig.xlsx"
INPUT_FILE = DATA_DIR / "df_nextbike_merged_mit_routen.csv"
GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"
OUT_FILE = DATA_DIR / "uebersicht_dauerradzaehler_korridore.csv"


def norm(text: object) -> str:
    text = unicodedata.normalize("NFKD", str(text))
    text = text.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", text.lower())


RULES = {
    100013246: {"radius": 20, "terms": ["renzstrae", "friedrichebertstrae"], "forced_edges": [(529049478, 1808551008, 0), (1808551008, 529049478, 0)]},
    100026599: {"radius": 45, "terms": ["jungbuschbrcke", "freherstrae"], "forced_edges": [(448897358, 10870394513, 0), (1176400818, 1068953392, 0), (1068953392, 1176400818, 0)]},
    100042618: {"radius": 35, "terms": ["lindenhofberfhrung"], "forced_edges": [(376624411, 861602217, 0), (861602217, 376624411, 0)]},
    100042619: {"radius": 50, "terms": ["neckarauer", "schwetzinger"]},
    100042620: {"radius": 55, "terms": ["schlossparklindenhof", "jugendherberge"]},
    100043759: {"radius": 70, "terms": ["konradadenauerbrucke", "rheinvorlandstrae"], "forced_edges": [(39956093, 1181466937, 0), (1181466937, 39956093, 0)]},
    100043761: {"radius": 45, "terms": ["kurpfalzbrcke", "brckenstrae"], "forced_edges": [(261599377, 1160134615, 0), (1160134615, 261599377, 0), (1160134615, 883463107, 0)]},
    300033853: {"radius": 35, "terms": ["feudenheimerstrae"]},
    300033855: {"radius": 35, "terms": ["feudenheimerstrae"]},
    300034898: {"radius": 35, "terms": ["luzenbergstrae"]},
    300034899: {"radius": 55, "terms": ["bbcbehelfsbrcke", "rollbhlstrae", "friedricheberstrae"], "forced_edges": [(1173649266, 1791270153, 0), (1791270153, 1173649266, 0)]},
    300034900: {"radius": 40, "terms": ["theodorheussanlage"]},
    300034901: {"radius": 40, "terms": ["theodorheussanlage"]},
    300034976: {"radius": 40, "terms": ["hansreschkeufer"]},
}


def route_edges(route: str, graph) -> set[tuple[int, int, int]]:
    try:
        coords = ast.literal_eval(route)
        if not isinstance(coords, list) or len(coords) < 2:
            return set()
        mids = [((a[0] + b[0]) / 2, (a[1] + b[1]) / 2) for a, b in zip(coords, coords[1:])]
        return set(ox.distance.nearest_edges(graph, X=[x for x, _ in mids], Y=[y for _, y in mids]))
    except Exception:
        return set()


def station_terms(name: str) -> list[str]:
    return [value for value in re.split(r"[^a-z0-9]+", norm(name)) if len(value) > 3]


def choose_corridor(graph, edges, point, radius, terms):
    candidates = edges[edges["geometry"].distance(point) <= radius].copy()
    if candidates.empty:
        nearest = edges.iloc[(edges["geometry"].distance(point)).argmin()]
        return {nearest.edge}, float(nearest.geometry.distance(point)), nearest.name

    candidates["dist"] = candidates["geometry"].distance(point)
    candidates["norm_name"] = candidates["name"].map(norm)
    candidate_match = candidates["norm_name"].apply(lambda value: any(term in value for term in terms)) if terms else pd.Series(False, index=candidates.index)

    candidate_edges = list(candidates["edge"])
    H = graph.edge_subgraph(candidate_edges).copy()
    if H.number_of_edges() == 0:
        return set(), float(candidates.iloc[0]["dist"]), None

    comps = list(nx.weakly_connected_components(H))
    named = []
    unnamed = []
    for comp in comps:
        comp_edges = candidates[candidates.apply(lambda row: row.u in comp and row.v in comp, axis=1)].copy()
        if comp_edges.empty:
            continue
        nearest_row = comp_edges.nsmallest(1, "dist").iloc[0]
        avg_dist = float(comp_edges["dist"].mean())
        corridor = set(comp_edges["edge"])
        name_match = bool(terms) and bool(candidate_match.loc[comp_edges.index].any())
        item = (avg_dist, float(nearest_row["dist"]), corridor, nearest_row)
        (named if name_match else unnamed).append(item)

    pool = named if named else unnamed
    if not pool:
        nearest = candidates.nsmallest(1, "dist").iloc[0]
        return {nearest.edge}, float(nearest.dist), nearest.name

    pool.sort(key=lambda item: (item[0], item[1], -len(item[2])))
    best = pool[0]
    return best[2], float(best[1]), best[3].get("name")


if __name__ == "__main__":
    print("🔄 Lade Overview-Datei...")
    overview = pd.read_excel(OVERVIEW_FILE)
    print(f"✅ {len(overview)} Stationen geladen")

    print("🔄 Lade Mannheim-Graph...")
    G0 = ox.load_graphml(GRAPH_FILE)
    G = ox.project_graph(G0)
    edges = ox.graph_to_gdfs(G, nodes=False, edges=True).reset_index()[["u", "v", "key", "name", "geometry"]]
    edges["edge"] = list(zip(edges.u, edges.v, edges.key))
    print(f"✅ Graph mit {len(edges)} Kanten geladen")

    print("🔄 Lade Edge-Hits-Cache...")
    cache_file = DATA_DIR / "cache" / "nextbike_edge_hits_cache.json"
    with open(cache_file) as f:
        cache_data = json.load(f)

    hits_list = cache_data.get("hits", [])
    edge_hits = {(u, v, k): count for u, v, k, count in hits_list}
    print(f"✅ Edge-Hits-Cache geladen ({len(edge_hits)} Edges mit Hits)")

    pts = gpd.GeoDataFrame(
        overview[["counter_site_id", "counter_site", "latitude", "longitude"]],
        geometry=gpd.points_from_xy(overview.longitude, overview.latitude),
        crs="EPSG:4326",
    ).to_crs(G.graph["crs"])

    print()
    print("🔄 Verarbeite Stationen...")
    rows = []
    for idx, row in enumerate(pts.itertuples(index=False), 1):
        rule = RULES.get(int(row.counter_site_id), {"radius": 40, "terms": []})
        terms = sorted(set(rule["terms"] + station_terms(row.counter_site)))

        forced = rule.get("forced_edges")
        if forced:
            corridor = set(tuple(e) for e in forced)
            mask = edges[edges["edge"].isin(corridor)]
            if not mask.empty:
                nearest_row = mask.iloc[0]
                nearest_dist = float(nearest_row.geometry.distance(row.geometry))
                nearest_name = nearest_row.name
            else:
                nearest_dist = None
                nearest_name = None
        else:
            corridor, nearest_dist, nearest_name = choose_corridor(G, edges, row.geometry, rule["radius"], terms)

        total = sum(edge_hits.get(edge, 0) for edge in corridor)

        rows.append(
            {
                "counter_site_id": int(row.counter_site_id),
                "counter_site": row.counter_site,
                "radius_m": rule["radius"],
                "corridor_edge_count": len(corridor),
                "nextbike_total": int(total),
                "nearest_edge_distance_m": round(nearest_dist, 3),
                "nearest_edge_name": nearest_name,
                "corridor_edges": " | ".join(str(edge) for edge in sorted(corridor)),
            }
        )
        print(f"  [{idx}/14] {row.counter_site_id}: {row.counter_site} -> edges={len(corridor)}, nextbike={int(total)}")

    print()
    pd.DataFrame(rows).sort_values(["counter_site_id"]).to_csv(OUT_FILE, index=False)
    print(f"✅ CSV gespeichert: {OUT_FILE}")
