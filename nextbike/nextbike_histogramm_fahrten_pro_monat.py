from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent  # mannheim_bike/
INPUT_FILE = DATA_DIR / "df_nextbike_merged_mit_routen.csv"
OUTPUT_PNG = DATA_DIR / "nextbike_fahrten_pro_monat_histogramm.png"

df = pd.read_csv(INPUT_FILE, usecols=["rueckgabe_datetime"])
df["rueckgabe_datetime"] = pd.to_datetime(df["rueckgabe_datetime"], errors="coerce")
df = df.dropna(subset=["rueckgabe_datetime"]).copy()

monthly = (
    df.assign(monat=df["rueckgabe_datetime"].dt.to_period("M").astype(str))
    .groupby("monat")
    .size()
    .sort_index()
)

plt.figure(figsize=(10, 5))
plt.bar(monthly.index, monthly.values, color="steelblue")
plt.title("Nextbike: Fahrten pro Monat")
plt.xlabel("Monat")
plt.ylabel("Anzahl Fahrten")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(OUTPUT_PNG, dpi=150)
plt.show()

print("Gespeichert:", OUTPUT_PNG)
print(monthly.to_string())
