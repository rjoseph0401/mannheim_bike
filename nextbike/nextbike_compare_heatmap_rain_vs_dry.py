from pathlib import Path
import ast
from collections import Counter

import osmnx as ox
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import openmeteo_requests
import pandas as pd
import requests_cache
from matplotlib.colors import TwoSlopeNorm
from matplotlib import cm
from retry_requests import retry

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
INPUT_FILE = DATA_DIR / "df_nextbike_merged_mit_routen.csv"
OUT_FILE = DATA_DIR / "nextbike_dry_minus_rainy.jpg"
GRAPH_FILE = DATA_DIR / "mannheim_bike.graphml"

RAINY_MIN_MM = 5.0
DRY_MAX_MM = 0.0
MAX_DAYS_PER_GROUP = 10


def parse_route(value):
    if not isinstance(value, str) or not value.strip().startswith("["):
        return None
    try:
        coords = ast.literal_eval(value)
        return coords if isinstance(coords, list) and len(coords) > 1 else None
    except Exception:
        return None


def load_graph():
    if GRAPH_FILE.exists():
        return ox.load_graphml(GRAPH_FILE)
    graph = ox.graph_from_place("Mannheim, Germany", network_type="bike", simplify=True)
    ox.save_graphml(graph, GRAPH_FILE)
    return graph


def get_daily_rain(start_date, end_date):
    cached = requests_cache.CachedSession(str(DATA_DIR / ".http_cache"), expire_after=3600)
    session = retry(cached, retries=2, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=session)
    response = client.weather_api(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": 49.487,
            "longitude": 8.466,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "daily": "precipitation_sum",
            "timezone": "Europe/Berlin",
        },
    )[0]
    daily = response.Daily()
    dates = pd.date_range(start_date, end_date, freq="D").date
    rain = np.array(daily.Variables(0).ValuesAsNumpy(), dtype=float)
    return pd.DataFrame({"date": dates, "prcp": rain})


def pick_day_groups(df):
    day_counts = df.groupby("date", as_index=False).size().rename(columns={"size": "trips"})
    weather = get_daily_rain(day_counts["date"].min(), day_counts["date"].max())
    daily = day_counts.merge(weather, on="date", how="inner")

    rainy = daily[daily["prcp"] >= RAINY_MIN_MM]
    dry = daily[daily["prcp"] <= DRY_MAX_MM]
    if rainy.empty or dry.empty:
        raise RuntimeError("Keine passenden Regen-/Trockentage gefunden.")

    n = min(MAX_DAYS_PER_GROUP, len(rainy), len(dry))
    # Deterministische Auswahl für reproduzierbare und vergleichbare Ergebnisse
    rainy_sel = rainy.sort_values(["prcp", "date"], ascending=[False, True]).head(n)
    dry_sel = dry.sort_values(["prcp", "date"], ascending=[True, True]).head(n)
    return rainy_sel, dry_sel


def edge_counts(df_subset, graph):
    hits = Counter()
    route_counts = df_subset["route_als_liste"].dropna().value_counts()
    all_x, all_y, route_ids = [], [], []
    for route, count in route_counts.items():
        coords = parse_route(route)
        if not coords or len(coords) < 2:
            continue
        mids = [((a[0] + b[0]) / 2, (a[1] + b[1]) / 2) for a, b in zip(coords, coords[1:])]
        for x, y in mids:
            all_x.append(x)
            all_y.append(y)
            route_ids.append((route, int(count)))

    if not all_x:
        return hits

    nearest = ox.distance.nearest_edges(graph, X=all_x, Y=all_y)
    seen = set()
    for (route, count), edge in zip(route_ids, nearest):
        key = (route, edge[0], edge[1], edge[2])
        if key in seen:
            continue
        seen.add(key)
        hits[edge] += count
    return hits


def main():
    df = pd.read_csv(
        INPUT_FILE,
        usecols=["rueckgabe_datetime", "start_lon", "start_lat", "end_lon", "end_lat", "route_als_liste"],
    )
    df["rueckgabe_datetime"] = pd.to_datetime(df["rueckgabe_datetime"], errors="coerce")
    df = df.dropna(subset=["rueckgabe_datetime", "start_lon", "start_lat", "end_lon", "end_lat"]).copy()
    df["date"] = df["rueckgabe_datetime"].dt.date
    df = df[df["rueckgabe_datetime"].dt.weekday <= 3].copy()
    if df.empty:
        raise RuntimeError("Keine Fahrten im verfügbaren Zeitraum (Mo-Do) gefunden.")

    rainy_days, dry_days = pick_day_groups(df)
    rainy_dates = set(rainy_days["date"].tolist())
    dry_dates = set(dry_days["date"].tolist())
    if len(rainy_dates) != len(dry_dates):
        raise RuntimeError("Ungleiche Anzahl von Regen- und Trockentagen.")
    n_days = max(1, len(rainy_dates))

    rainy_df = df[df["date"].isin(rainy_dates)].copy()
    dry_df = df[df["date"].isin(dry_dates)].copy()

    graph = load_graph()
    edges = ox.graph_to_gdfs(graph, nodes=False, edges=True).copy()

    rainy_hits = edge_counts(rainy_df, graph)
    dry_hits = edge_counts(dry_df, graph)

    all_edges = set(rainy_hits) | set(dry_hits)
    diff_rows = []
    for edge in all_edges:
        rainy_per_day = rainy_hits.get(edge, 0) / n_days
        dry_per_day = dry_hits.get(edge, 0) / n_days
        diff_rows.append(
            {
                "edge": edge,
                "geometry": edges.loc[edge].geometry,
                "delta": float(dry_per_day - rainy_per_day),
            }
        )
    diff = pd.DataFrame(diff_rows)
    diff = diff[diff["geometry"].notna()].copy()

    abs_delta = np.abs(diff["delta"].values)
    vmax = float(np.percentile(abs_delta, 95)) if len(abs_delta) else 1.0
    vmax = max(vmax, 1.0)
    norm = TwoSlopeNorm(vmin=-vmax, vcenter=0.0, vmax=vmax)
    cmap = plt.get_cmap("RdBu_r")

    fig, ax = ox.plot_graph(
        graph,
        node_size=0,
        edge_color="gray",
        edge_linewidth=0.4,
        bgcolor="white",
        show=False,
        close=False,
        figsize=(14, 14),
    )
    for r in diff.itertuples():
        strength = min(abs(r.delta) / vmax, 1.0) if vmax > 0 else 0.0
        if strength < 0.02:
            continue
        geom = r.geometry
        line_list = [geom] if geom.geom_type == "LineString" else list(geom.geoms)
        for line in line_list:
            ax.plot(
                *line.xy,
                color=mcolors.to_hex(cmap(norm(r.delta))),
                linewidth=1.2 + 2.0 * np.sqrt(strength),
                alpha=0.9,
            )

    cbar = fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax, fraction=0.03, pad=0.01)
    cbar.set_label("Dry minus Rainy (Fahrten pro Tag je Kante)")
    rainy_trip_count = int(len(rainy_df))
    dry_trip_count = int(len(dry_df))
    ax.set_title(
        f"Dry minus Rainy (Mo-Do) | Regen-Tage: {len(rainy_dates)} ({rainy_trip_count}) | "
        f"Trocken-Tage: {len(dry_dates)} ({dry_trip_count})"
    )
    fig.savefig(OUT_FILE, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print("Gespeichert:", OUT_FILE)
    print(f"Ausgewählte Regen-Tage ({len(rainy_dates)}):", sorted(rainy_dates))
    print(f"Ausgewählte trockene Tage ({len(dry_dates)}):", sorted(dry_dates))
    print(f"Fahrten rainy: {rainy_trip_count}")
    print(f"Fahrten dry: {dry_trip_count}")
    print(f"Diff-Skala (Kantenhits pro Tag, 95. Perzentil): +/- {vmax:.2f}")


if __name__ == "__main__":
    main()
