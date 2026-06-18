"""
Berechne Quotienten: Nextbike-Verkehr / Gesamtverkehr (Dauerzählstellen)
für den Nextbike-Zeitraum (2025-03-06 bis 2025-10-06).

Lädt Excel-Dateien der Zählstellen von GitHub und vergleicht gegen
die berechneten Nextbike-Korridore.

Hinweis: Diese Datei ist weitgehend durch dauerzaehl_nextbike_match.py ersetzt.
"""

from __future__ import annotations

import re
import pandas as pd
import numpy as np
from pathlib import Path
from urllib.request import urlopen
from urllib.parse import quote
from io import BytesIO

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/

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

def load_excel_from_github(filename: str) -> pd.DataFrame | None:
    try:
        encoded_filename = quote(filename, safe='')
        url = f"{GITHUB_BASE}/{encoded_filename}"
        with urlopen(url) as response:
            excel_data = BytesIO(response.read())
        return pd.read_excel(excel_data, skiprows=3)
    except Exception as e:
        print(f"  ⚠️  Fehler beim Laden von {filename}: {e}")
        return None

def extract_date_column(df: pd.DataFrame) -> pd.DataFrame | None:
    try:
        first_col = df.columns[0]
        df[first_col] = pd.to_datetime(df[first_col], errors='coerce')
        df = df.dropna(subset=[first_col])
        if df.empty:
            return None
        return df
    except Exception as e:
        print(f"    Fehler beim Parsen der Datumsspalte: {e}")
        return None

def filter_and_sum_counter(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> int:
    try:
        date_col = df.columns[0]
        mask = (df[date_col] >= start) & (df[date_col] <= end)
        filtered = df.loc[mask]
        if filtered.empty:
            return 0
        numeric_cols = list(filtered.select_dtypes(include=[np.number]).columns)
        if not numeric_cols:
            return 0
        in_cols = [c for c in numeric_cols if re.search(r'\bIN\b|\bin\b| in$|\.in$|_in$', c, flags=re.I)]
        out_cols = [c for c in numeric_cols if re.search(r'\bOUT\b|\bout\b| out$|\.out$|_out$', c, flags=re.I)]
        total_candidates = [c for c in numeric_cols if c not in in_cols + out_cols]
        if total_candidates:
            for cand in total_candidates:
                cand_sum = filtered[cand].sum()
                inout_sum = 0
                if in_cols and out_cols:
                    inout_sum = filtered[in_cols].sum().sum() + filtered[out_cols].sum().sum()
                if inout_sum > 0 and abs(cand_sum - inout_sum) / max(1, inout_sum) < 0.02:
                    return int(cand_sum)
            cand_sums = {c: filtered[c].sum() for c in total_candidates}
            best = max(cand_sums, key=cand_sums.get)
            return int(cand_sums[best])
        if in_cols and out_cols:
            return int(filtered[in_cols].sum().sum() + filtered[out_cols].sum().sum())
        return int(filtered[numeric_cols].sum().sum())
    except Exception as e:
        print(f"    Fehler beim Summieren: {e}")
        return 0

def main():
    corridor_file = DATA_DIR / "uebersicht_dauerradzaehler_korridore.csv"

    if not corridor_file.exists():
        print(f"❌ {corridor_file} existiert nicht!")
        print("  Bitte führe zuerst 'uebersicht_dauerradzaehler_korridore.py' aus.")
        return

    nextbike_df = pd.read_csv(corridor_file)
    print(f"✅ Nextbike Korridore geladen ({len(nextbike_df)} Stationen)")
    print()

    results = []

    for _, row in nextbike_df.iterrows():
        station_id = int(row["counter_site_id"])
        station_name = row["counter_site"]
        nextbike_total = int(row["nextbike_total"])

        print(f"📍 {station_name} (ID: {station_id})")

        files = COUNTER_FILES.get(station_id, [])
        if not files:
            print(f"  ⚠️  Keine Counter-Datei definiert")
            results.append({
                "counter_site_id": station_id,
                "counter_site": station_name,
                "nextbike_total": nextbike_total,
                "counter_total": None,
                "quotient": None,
                "note": "Keine Counter-Datei definiert"
            })
            continue

        counter_total = 0
        loaded_files = []

        for filename in files:
            print(f"  📥 Lade {filename}...")
            df = load_excel_from_github(filename)
            if df is None:
                print(f"    ⚠️  Konnte nicht geladen werden")
                continue
            df = extract_date_column(df)
            if df is None:
                print(f"    ⚠️  Konnte Datumsspalte nicht parsen")
                continue
            total = filter_and_sum_counter(df, NEXTBIKE_START, NEXTBIKE_END)
            counter_total += total
            loaded_files.append((filename, total))
            print(f"    ✅ {total:,} Fahrten im Nextbike-Zeitraum")

        if counter_total > 0:
            quotient = nextbike_total / counter_total
            status = "✅"
        else:
            quotient = None
            status = "⚠️ "

        print(f"  {status} Nextbike: {nextbike_total:,} | Counter: {counter_total:,} | Quotient: {quotient:.2%}" if quotient else f"{status} Counter hat 0 Fahrten")
        print()

        results.append({
            "counter_site_id": station_id,
            "counter_site": station_name,
            "nextbike_total": nextbike_total,
            "counter_total": counter_total if counter_total > 0 else None,
            "quotient": quotient,
            "note": f"Dateien: {', '.join([f[0] for f in loaded_files])}" if loaded_files else "Keine Dateien geladen",
            "sort_order": len(results)
        })

    combined_rows = []
    results_df = pd.DataFrame(results)
    for combined_name, station_ids in COMBINED_STATIONS.items():
        subset = results_df[results_df["counter_site_id"].isin(station_ids)]
        if subset.empty:
            continue
        nextbike_total = int(subset["nextbike_total"].sum())
        counter_total = int(subset["counter_total"].fillna(0).sum())
        quotient = nextbike_total / counter_total if counter_total > 0 else None
        notes = ", ".join(subset["note"].astype(str).tolist())
        combined_rows.append({
            "counter_site_id": "+".join(str(sid) for sid in station_ids),
            "counter_site": combined_name,
            "nextbike_total": nextbike_total,
            "counter_total": counter_total if counter_total > 0 else None,
            "quotient": quotient,
            "note": f"Kombiniert aus: {notes}",
            "sort_order": int(subset["sort_order"].min()),
        })

    if combined_rows:
        combined_station_ids = {sid for station_ids in COMBINED_STATIONS.values() for sid in station_ids}
        results_df = results_df[~results_df["counter_site_id"].isin(combined_station_ids)]
        results_df = pd.concat([results_df, pd.DataFrame(combined_rows)], ignore_index=True)
        results_df = results_df.sort_values(["sort_order", "counter_site"], kind="stable")
    else:
        results_df = results_df.sort_values(["sort_order", "counter_site"], kind="stable")

    output_file = DATA_DIR / "quotient_nextbike_vs_zaehler.csv"
    results_df.drop(columns=["sort_order"]).to_csv(output_file, index=False)

    print("=" * 80)
    print(f"✅ Ergebnisse gespeichert: {output_file}")
    print()
    print(results_df.to_string(index=False))

if __name__ == "__main__":
    main()
