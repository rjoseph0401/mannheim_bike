"""
Statische Analysen der Stadtradeln-Daten (unabhängig vom Routing-Algorithmus).

Plots:
1. Jahresvergleich Gesamtkilometer (gewichtet)
"""

import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

YEARS = [2022, 2023, 2024]
DATA_DIR = Path("Data/outputs")
OUT_DIR = Path("Results")
OUT_DIR.mkdir(exist_ok=True)

TOP_N = 15
PROJ_CRS = "EPSG:25832"


def load_year(year: int) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(DATA_DIR / f"stadtradeln_graphhopper_routes_{year}.gpkg")
    gdf = gdf.to_crs(PROJ_CRS)
    gdf["seg_len_m"] = gdf.geometry.length
    od = (
        gdf.groupby(["key", "slon", "slat", "elon", "elat"])
        .agg(route_len_m=("seg_len_m", "sum"), trips=("trips", "first"))
        .reset_index()
    )
    return od


print("Lade Daten...")
data = {year: load_year(year) for year in YEARS}

# ── 1. Jahresvergleich Gesamtkilometer ──────────────────────────────────────

total_km = {
    year: (data[year]["route_len_m"] * data[year]["trips"]).sum() / 1_000
    for year in YEARS
}

fig, ax = plt.subplots(figsize=(7, 5))
bars = ax.bar(
    [str(y) for y in YEARS],
    [total_km[y] for y in YEARS],
    color="#2e86ab",
    width=0.5,
)
for bar, val in zip(bars, [total_km[y] for y in YEARS]):
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + 2_000,
        f"{val:,.0f} km",
        ha="center",
        va="bottom",
        fontsize=9,
    )
ax.set_title("Stadtradeln: Gesamte gefahrene Strecke pro Jahr", fontsize=12)
ax.set_xlabel("Jahr")
ax.set_ylabel("Gesamtkilometer (gewichtet)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
ax.set_ylim(0, max(total_km.values()) * 1.15)
plt.tight_layout()
plt.savefig(OUT_DIR / "stadtradeln_gesamtkilometer.png", dpi=150, facecolor="white")
plt.close()
print("OK stadtradeln_gesamtkilometer.png")

print("\nAlle Plots gespeichert in:", OUT_DIR)
