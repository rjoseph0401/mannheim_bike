"""
Sensitivitätsanalyse der Transektbreite an den drei Problemstellen

Neckarauer Übergang, B38 und Theodor-Heuss-Anlage sind die drei Stationen,
bei denen die automatische Richtungsschätzung (auto_bearing) besonders
fehleranfällig ist — entweder weil die Straße schräg verläuft, mehrere
Routen aus verschiedenen Richtungen zusammentreffen oder die Referenzzählung
erst ab 2023 vorliegt. Deshalb wird hier gezielt für diese Problemstellen
untersucht, wie empfindlich die berechnete Ratio auf die Wahl von
TRANSECT_HALF_W reagiert.

Ablauf:
  1. Für jede Halbbreite von 1 bis 50 m wird das Transekt neu konstruiert.
  2. Pro Jahr wird gezählt, wie viele simulierte Trips das Transekt schneiden,
     und die Ratio (Simulation / Dauerzählung) berechnet.
  3. Die Jahres-Ratios werden gemittelt und als Kurve über die Breite geplottet.
  4. Eine gestrichelte Linie markiert den aktuell genutzten Wert (25 m).

Interpretation: Flache Kurven ab einem bestimmten Wert zeigen, dass das
Ergebnis stabil ist; starke Steigungen deuten auf Sensitivität hin.
"""
import numpy as np
import matplotlib.pyplot as plt
from query_locations import LOCATIONS, gdfs, _gdf_ref, make_transect, auto_bearing, pts

HALF_W_RANGE = range(1, 51)
TARGET_NAMES = {"Neckarauer Übergang", "B38", "Theodor-Heuss-Anlage"}

# Stationen und ihre vorberechneten Bearings + Referenzwerte herausziehen
stations = []
for loc, pt in zip(LOCATIONS, pts.geometry):
    name, lat, lon, refs, bearing_override = loc
    if name not in TARGET_NAMES:
        continue
    bearing = bearing_override if bearing_override is not None else auto_bearing(pt, _gdf_ref)
    stations.append((name, pt, bearing, refs))

results = {s[0]: [] for s in stations}

for half_w in HALF_W_RANGE:
    for name, pt, bearing, refs in stations:
        transect = make_transect(pt, bearing, half_w=half_w)
        ratios = []
        for year, gdf in gdfs.items():
            ref = refs.get(year)
            if not ref:
                continue
            hits  = gdf.sindex.query(transect, predicate="intersects")
            trips = int(gdf.iloc[hits]["trips"].sum())
            ratios.append(trips / ref)
        results[name].append(np.mean(ratios) if ratios else np.nan)

fig, ax = plt.subplots(figsize=(9, 5))
colors = {"Neckarauer Übergang": "#2ecc71", "B38": "#e74c3c", "Theodor-Heuss-Anlage": "#3498db"}

for name, vals in results.items():
    ax.plot(list(HALF_W_RANGE), vals, label=name, color=colors[name], linewidth=2)

ax.axvline(x=25, color="gray", linestyle="--", linewidth=1, label="aktuell (25 m)")
ax.set_xlabel("TRANSECT_HALF_W [m]")
ax.set_ylabel("Ø Ratio Stadtradeln / Dauerzählstelle")
ax.set_xlim(1, 50)
ax.set_ylim(bottom=0)
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.0%}"))
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("Results/transect_sensitivity.png", dpi=150)
plt.show()
print("Gespeichert: Results/transect_sensitivity.png")
