from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
INPUT_FILE = DATA_DIR / "df_nextbike_merged_mit_routen.csv"
OUTPUT_PNG = DATA_DIR / "nextbike_fahrten_pro_wochentag_histogramm.png"

df = pd.read_csv(INPUT_FILE, usecols=["rueckgabe_datetime"])
df["rueckgabe_datetime"] = pd.to_datetime(df["rueckgabe_datetime"], errors="coerce")
df = df.dropna(subset=["rueckgabe_datetime"]).copy()

weekday_order = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
weekday = (
    df.assign(wochentag=df["rueckgabe_datetime"].dt.dayofweek)
    .groupby("wochentag")
    .size()
    .reindex(range(7), fill_value=0)
)

plt.figure(figsize=(9, 5))
plt.bar(weekday_order, weekday.values, color="seagreen")
plt.title("Nextbike: Fahrten pro Wochentag")
plt.xlabel("Wochentag")
plt.ylabel("Anzahl Fahrten")
plt.xticks(rotation=20, ha="right")
plt.tight_layout()
plt.savefig(OUTPUT_PNG, dpi=150)
plt.show()

print("Gespeichert:", OUTPUT_PNG)
print(pd.Series(weekday.values, index=weekday_order).to_string())
