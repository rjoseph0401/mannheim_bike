"""Debug-Skript: Überprüft Route-Edge-Matching für ausgewählte Stationen."""

import ast
from pathlib import Path
import pandas as pd
import osmnx as ox

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
ueb = DATA_DIR / "uebersicht_dauerradzaehler_korridore.csv"
trips_file = DATA_DIR / "df_nextbike_merged_mit_routen.csv"
G_file = DATA_DIR / "mannheim_bike.graphml"

print('Loading uebersicht...')
df = pd.read_csv(ueb)
rows = {int(r.counter_site_id): r for r in df.itertuples(index=False)}

print('Loading graph...')
G0 = ox.load_graphml(G_file)

print('Loading trips (unique routes)...')
trips = pd.read_csv(trips_file, usecols=['route_als_liste']).dropna()
route_counts = trips['route_als_liste'].value_counts()
unique_routes = list(route_counts.index)

print('Building route->edge cache for', len(unique_routes), 'unique routes')

def route_edges(route):
    try:
        coords = ast.literal_eval(route)
        if not isinstance(coords, list) or len(coords) < 2:
            return set()
        mids = [((a[0]+b[0])/2, (a[1]+b[1])/2) for a, b in zip(coords, coords[1:])]
        return set(ox.distance.nearest_edges(G0, X=[x for x, _ in mids], Y=[y for _, y in mids]))
    except Exception:
        return set()

route_cache = {}
for i, r in enumerate(unique_routes, 1):
    route_cache[r] = route_edges(r)
    if i % 1000 == 0:
        print(' processed', i)

targets = [100013246, 100042618, 300034899]
for sid in targets:
    row = rows[sid]
    corridor_str = row.corridor_edges
    parts = [p.strip() for p in corridor_str.split('|')]
    corridor = set()
    for p in parts:
        try:
            t = ast.literal_eval(p)
            if isinstance(t, tuple):
                corridor.add(t)
        except Exception:
            pass
    total = 0
    for route, cnt in route_counts.items():
        if route_cache.get(route, set()) & corridor:
            total += cnt
    print('\nStation', sid, row.counter_site)
    print(' corridor edges:', len(corridor))
    print(' unique-route based nextbike total:', total)
