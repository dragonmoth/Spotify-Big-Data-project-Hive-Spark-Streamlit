"""
Turns the Spark result CSVs into PNG charts for the report.
Run in Ubuntu:  python3 /mnt/c/bda/make_charts.py
Reads  /mnt/c/bda/results/<analysis>/part-*.csv
Writes /mnt/c/bda/charts/*.png
"""
import glob
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

RESULTS = "/mnt/c/bda/results"
CHARTS = "/mnt/c/bda/charts"
os.makedirs(CHARTS, exist_ok=True)


def load(name):
    files = glob.glob(f"{RESULTS}/{name}/part-*.csv")
    if not files:
        raise FileNotFoundError(f"No CSV found in {RESULTS}/{name}")
    return pd.read_csv(files[0])


def save(fig, name):
    fig.tight_layout()
    fig.savefig(f"{CHARTS}/{name}.png", dpi=150)
    plt.close(fig)
    print("saved", name)


def hbar(df, label, value, title, xlabel, name, n=15, color="#1DB954"):
    d = df.sort_values(value, ascending=False).head(n).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(d[label].astype(str), d[value], color=color)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    save(fig, name)


# 1. Top artists by chart appearances
artists = load("02_top_artists")
hbar(artists, "artist", "appearances", "Top 15 artists by chart appearances",
     "Daily chart appearances (all countries)", "01_top_artists")

# 2. Songs charting in the most countries
reach = load("03_song_reach")
reach["label"] = reach["name"].str.slice(0, 35) + " - " + reach["artists"].str.split(",").str[0]
hbar(reach, "label", "countries", "Songs charting in the most countries",
     "Number of countries", "02_song_reach", color="#191414")

# 3. Monthly global mood trend
monthly = load("05_monthly_global_trend").sort_values("month")
fig, ax = plt.subplots(figsize=(10, 5))
for col in ["valence", "energy", "danceability"]:
    ax.plot(monthly["month"], monthly[col], marker="o", label=col)
ax.set_title("Global top-50: average audio features by month")
ax.set_ylabel("Average value (0 to 1)")
ax.tick_params(axis="x", rotation=60)
ax.legend()
save(fig, "03_monthly_trend")

# 4. Country profiles: happiest vs. saddest, and explicit share
country = load("04_country_profile")
country = country[country["country"] != "GLOBAL"]
hbar(country, "country", "valence", "Countries with the most positive music (avg valence)",
     "Average valence", "04_country_valence_top", n=15, color="#F5A623")
hbar(country, "country", "explicit_pct", "Countries with the highest share of explicit songs",
     "% explicit chart entries", "05_country_explicit", n=15, color="#E8575A")

# 5. Which audio features relate to popularity
corr = load("06_popularity_correlation").sort_values("corr_with_popularity")
fig, ax = plt.subplots(figsize=(8, 5))
colors = ["#E8575A" if v < 0 else "#1DB954" for v in corr["corr_with_popularity"]]
ax.barh(corr["feature"], corr["corr_with_popularity"], color=colors)
ax.axvline(0, color="black", linewidth=0.8)
ax.set_title("Correlation of audio features with popularity")
ax.set_xlabel("Pearson correlation")
save(fig, "06_popularity_correlation")

# 6. KMeans clusters: size and feature profile (z-scored heatmap)
prof = load("07_cluster_profiles").sort_values("cluster")
fig, ax = plt.subplots(figsize=(6, 4))
ax.bar(prof["cluster"].astype(str), prof["songs"], color="#1DB954")
ax.set_title("Songs per cluster")
ax.set_xlabel("Cluster")
ax.set_ylabel("Songs")
save(fig, "07_cluster_sizes")

features = [c for c in prof.columns if c not in ("cluster", "songs")]
z = (prof[features] - prof[features].mean()) / prof[features].std(ddof=0)
fig, ax = plt.subplots(figsize=(9, 4))
im = ax.imshow(z.values, cmap="RdYlGn", aspect="auto")
ax.set_xticks(range(len(features)))
ax.set_xticklabels(features, rotation=45, ha="right")
ax.set_yticks(range(len(prof)))
ax.set_yticklabels([f"Cluster {c}" for c in prof["cluster"]])
for i in range(z.shape[0]):
    for j in range(z.shape[1]):
        ax.text(j, i, f"{z.values[i, j]:.1f}", ha="center", va="center", fontsize=8)
ax.set_title("Cluster profiles (z-score of each feature across clusters)")
fig.colorbar(im, ax=ax)
save(fig, "08_cluster_profiles")

print("Done. Charts are in", CHARTS)
