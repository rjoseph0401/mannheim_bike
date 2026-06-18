from __future__ import annotations

import geopandas as gpd
import pandas as pd

from load_counter_and_nextbike_data import (
    DATA_DIR,
    COUNTER_FILES,
    COMBINED_STATIONS,
    STATION_RADII_M,
    NEXTBIKE_START,
    NEXTBIKE_END,
    load_all_station_data,
    load_excel_from_github,
    parse_datetime_column,
    sum_counter_in_range,
    choose_corridor_component,
)

OUT_MATCHED = DATA_DIR / "matched_edges_nextbike_gesamt.csv"
OUT_QUOTIENT = DATA_DIR / "quotient_nextbike_vs_zaehler.csv"


def main() -> None:
    data = load_all_station_data()
    stations = data["stations"]
    graph = data["graph"]
    edges = data["edges"]
    edge_hits = data["edge_hits"]

    print()
    print("🔄 Berechne Nextbike-Überfahrten je Zählstelle...")
    matched_rows = []
    matched_export = []
    for idx, row in enumerate(stations.itertuples(index=False), 1):
        point = gpd.GeoSeries([row.geometry], crs=stations.crs).to_crs(graph.graph["crs"]).iloc[0]
        radius_m = STATION_RADII_M.get(int(row.counter_site_id), 50)
        corridor, total, distance_m, edge_name = choose_corridor_component(graph, edges, point, radius_m, edge_hits)
        rep_edge = sorted(corridor)[0]

        matched_rows.append(
            {
                "counter_site_id": int(row.counter_site_id),
                "counter_site": row.counter_site,
                "nextbike_total": int(total),
                "distance_m": round(distance_m, 3),
                "edge_name": edge_name,
                "edge": rep_edge,
                "corridor_edges": corridor,
                "radius_m": radius_m,
            }
        )
        matched_export.append(
            {
                "counter_site_id": int(row.counter_site_id),
                "counter_site": row.counter_site,
                "latitude": float(row.latitude),
                "longitude": float(row.longitude),
                "u": rep_edge[0],
                "v": rep_edge[1],
                "k": rep_edge[2],
                "radius_m": radius_m,
                "anzahl_nextbike_ueberfahrten": int(total),
                "distance_m": round(distance_m, 3),
                "edge_name": edge_name,
                "corridor_edge_count": len(corridor),
            }
        )
        print(f"  [{idx}/14] {row.counter_site_id}: {row.counter_site} -> radius={radius_m}m, edges={len(corridor)}, total={int(total)}")

    pd.DataFrame(matched_export).to_csv(OUT_MATCHED, index=False)
    print(f"✅ Rohdaten gespeichert: {OUT_MATCHED}")

    print()
    print("🔄 Lade Counter-Dateien und berechne Quotienten...")
    station_lookup = {row["counter_site_id"]: row for row in matched_rows}
    results = []

    for station_id, files in COUNTER_FILES.items():
        row = station_lookup.get(station_id)
        if row is None:
            continue

        print(f"📍 {row['counter_site']} (ID: {station_id})")
        counter_total = 0
        loaded = []
        for filename in files:
            print(f"  📥 Lade {filename}...")
            df = load_excel_from_github(filename)
            if df is None:
                print("    ⚠️  Konnte nicht geladen werden")
                continue
            df = parse_datetime_column(df)
            if df is None:
                print("    ⚠️  Konnte Datumsspalte nicht parsen")
                continue
            total = sum_counter_in_range(df, NEXTBIKE_START, NEXTBIKE_END)
            counter_total += total
            loaded.append(filename)
            print(f"    ✅ {total:,} Fahrten im Nextbike-Zeitraum")

        quotient = row["nextbike_total"] / counter_total if counter_total > 0 else None
        if quotient is not None:
            print(f"  ✅ Nextbike: {row['nextbike_total']:,} | Counter: {counter_total:,} | Quotient: {quotient:.2%}")
        else:
            print("  ⚠️  Counter hat 0 Fahrten")
        print()

        results.append(
            {
                "counter_site_id": station_id,
                "counter_site": row["counter_site"],
                "nextbike_total": int(row["nextbike_total"]),
                "counter_total": counter_total if counter_total > 0 else None,
                "quotient": quotient,
                "note": f"Dateien: {', '.join(loaded)}" if loaded else "Keine Dateien geladen",
            }
        )

    results_df = pd.DataFrame(results)
    grouped_rows = []
    for combined_name, station_ids in COMBINED_STATIONS.items():
        subset = results_df[results_df["counter_site_id"].isin(station_ids)]
        if subset.empty:
            continue

        nextbike_total = int(subset["nextbike_total"].sum())
        counter_total = int(subset["counter_total"].fillna(0).sum())
        grouped_rows.append(
            {
                "counter_site_id": "+".join(str(sid) for sid in station_ids),
                "counter_site": combined_name,
                "nextbike_total": nextbike_total,
                "counter_total": counter_total if counter_total > 0 else None,
                "quotient": nextbike_total / counter_total if counter_total > 0 else None,
                "note": f"Kombiniert aus: {', '.join(subset['note'].astype(str))}",
            }
        )

    if grouped_rows:
        remove_ids = {sid for ids in COMBINED_STATIONS.values() for sid in ids}
        results_df = results_df[~results_df["counter_site_id"].isin(remove_ids)]
        results_df = pd.concat([results_df, pd.DataFrame(grouped_rows)], ignore_index=True)

    results_df = results_df.sort_values(["counter_site_id", "counter_site"], kind="stable")

    total_nextbike_sum = results_df["nextbike_total"].sum()
    results_df["quot_nextbike_vs_gesamt_pct"] = (
        results_df["nextbike_total"] / total_nextbike_sum * 100
    ).round(2)
    total_counter_sum = results_df["counter_total"].fillna(0).sum()
    results_df["quot_zaehler_vs_gesamt_pct"] = (
        results_df["counter_total"] / total_counter_sum * 100
    ).round(2)

    results_df.to_csv(OUT_QUOTIENT, index=False)

    print("=" * 80)
    print(f"✅ Quotienten gespeichert: {OUT_QUOTIENT}")
    print(f"   Summe quot_nextbike_vs_gesamt_pct:  {results_df['quot_nextbike_vs_gesamt_pct'].sum():.2f}%")
    print(f"   Summe quot_zaehler_vs_gesamt_pct:  {results_df['quot_zaehler_vs_gesamt_pct'].sum():.2f}%")
    print()
    print(results_df.to_string(index=False))


if __name__ == "__main__":
    main()
