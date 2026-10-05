"""
Spotify Top 50 - interactive big-data dashboard.
Every number is computed live by Spark SQL on the cleaned Hive (ORC) table in HDFS.
Run:  streamlit run /mnt/c/bda/app.py
"""
import glob

import pandas as pd
import plotly.express as px
import streamlit as st
from pyspark import StorageLevel
from pyspark.sql import SparkSession

CLEAN = "hdfs://localhost:9000/user/hive/warehouse/spotify_bda.db/spotify_clean"
RESULTS = "/mnt/c/bda/results"
FEATURES = ["danceability", "energy", "valence", "acousticness", "speechiness",
            "liveness", "tempo", "loudness", "popularity"]

st.set_page_config(page_title="Spotify Top 50 - Big Data", layout="wide")


@st.cache_resource(show_spinner="Starting Spark and loading 2.1M rows from HDFS...")
def get_spark():
    spark = (SparkSession.builder.master("local[2]").appName("SpotifyDashboard")
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.orc(CLEAN).persist(StorageLevel.MEMORY_AND_DISK)
    df.createOrReplaceTempView("spotify")
    df.count()
    return spark


@st.cache_data(show_spinner="Running Spark SQL...")
def q(sql: str) -> pd.DataFrame:
    return get_spark().sql(sql).toPandas()


get_spark()
bounds = q("SELECT MIN(snapshot_date) lo, MAX(snapshot_date) hi FROM spotify").iloc[0]
countries = q("SELECT DISTINCT country FROM spotify ORDER BY country")["country"].tolist()

# ---------------- sidebar filters ----------------
st.sidebar.header("Filters")
country = st.sidebar.selectbox("Country", ["All"] + countries)
d1, d2 = st.sidebar.date_input("Date range", (bounds.lo, bounds.hi),
                               min_value=bounds.lo, max_value=bounds.hi)
artist = st.sidebar.text_input("Artist contains").replace("'", "").lower()

conds = [f"snapshot_date BETWEEN '{d1}' AND '{d2}'"]
if country != "All":
    conds.append(f"country = '{country}'")
if artist:
    conds.append(f"lower(artists) LIKE '%{artist}%'")
W = "WHERE " + " AND ".join(conds)

st.title("Spotify Top 50: live big-data analysis")
st.caption("Data: CSV to HDFS to Hive (cleaned ORC) to Spark SQL. Filters apply to every tab except the SQL console.")

k = q(f"""SELECT COUNT(*) rows_, COUNT(DISTINCT spotify_id) songs, COUNT(DISTINCT country) countries,
          ROUND(AVG(valence),3) valence, ROUND(100*AVG(CAST(is_explicit AS INT)),1) explicit
          FROM spotify {W}""").iloc[0]
c = st.columns(5)
for col, (label, val) in zip(c, [("Chart entries", f"{int(k.rows_):,}"), ("Unique songs", f"{int(k.songs):,}"),
                                 ("Countries", int(k.countries)), ("Avg valence", k.valence),
                                 ("Explicit %", k.explicit)]):
    col.metric(label, val)

tabs = st.tabs(["Artists", "Trends", "Compare countries", "Song explorer", "SQL console", "Spark ML results"])

# ---------------- Artists ----------------
with tabs[0]:
    n = st.slider("How many artists", 5, 40, 15)
    a = q(f"""SELECT trim(a) artist, COUNT(*) appearances, COUNT(DISTINCT spotify_id) songs,
              ROUND(AVG(daily_rank),1) avg_rank
              FROM spotify LATERAL VIEW explode(split(artists, ',')) t AS a {W}
              GROUP BY trim(a) ORDER BY appearances DESC LIMIT {n}""")
    st.plotly_chart(px.bar(a.iloc[::-1], x="appearances", y="artist", orientation="h",
                           hover_data=["songs", "avg_rank"]), use_container_width=True)
    st.dataframe(a, use_container_width=True)

# ---------------- Trends ----------------
with tabs[1]:
    feats = st.multiselect("Audio features", FEATURES[:7], ["valence", "energy", "danceability"])
    grain = st.radio("Group by", ["month", "week"], horizontal=True)
    fmt = "yyyy-MM" if grain == "month" else "yyyy-ww"
    if feats:
        t = q(f"""SELECT date_format(snapshot_date,'{fmt}') period,
                  {', '.join(f'ROUND(AVG({f}),3) {f}' for f in feats)}
                  FROM spotify {W} GROUP BY date_format(snapshot_date,'{fmt}') ORDER BY period""")
        st.plotly_chart(px.line(t, x="period", y=feats, markers=True), use_container_width=True)

# ---------------- Compare countries ----------------
with tabs[2]:
    f1 = st.selectbox("Feature", FEATURES + ["explicit_pct"], index=2)
    expr = "100*AVG(CAST(is_explicit AS INT))" if f1 == "explicit_pct" else f"AVG({f1})"
    cmp = q(f"""SELECT country, ROUND({expr},3) value, COUNT(*) entries FROM spotify
                WHERE snapshot_date BETWEEN '{d1}' AND '{d2}' AND country <> 'GLOBAL'
                GROUP BY country ORDER BY value DESC""")
    st.plotly_chart(px.bar(cmp, x="country", y="value", hover_data=["entries"]), use_container_width=True)
    f2 = st.selectbox("Scatter: second feature", ["energy", "danceability", "acousticness", "speechiness"], index=0)
    sc = q(f"""SELECT country, AVG({'valence' if f1 in ('explicit_pct','valence') else f1}) x, AVG({f2}) y
               FROM spotify WHERE country <> 'GLOBAL' GROUP BY country""")
    st.plotly_chart(px.scatter(sc, x="x", y="y", text="country",
                               labels={"x": "valence" if f1 in ("explicit_pct", "valence") else f1, "y": f2}),
                    use_container_width=True)

# ---------------- Song explorer ----------------
with tabs[3]:
    s = st.text_input("Search song title").replace("'", "").lower()
    if s:
        songs = q(f"""SELECT spotify_id, name, artists, COUNT(DISTINCT country) countries,
                      COUNT(DISTINCT snapshot_date) days_charted, MIN(daily_rank) best_rank
                      FROM spotify WHERE lower(name) LIKE '%{s}%'
                      GROUP BY spotify_id, name, artists ORDER BY days_charted DESC LIMIT 20""")
        st.dataframe(songs, use_container_width=True)
        if len(songs):
            pick = st.selectbox("Show rank history for", songs["spotify_id"],
                                format_func=lambda i: songs.set_index("spotify_id").loc[i, "name"])
            hist = q(f"""SELECT snapshot_date, country, daily_rank FROM spotify
                         WHERE spotify_id = '{pick}' AND country IN ('GLOBAL','US','GB','IN','BR','MX')
                         ORDER BY snapshot_date""")
            fig = px.line(hist, x="snapshot_date", y="daily_rank", color="country")
            fig.update_yaxes(autorange="reversed", title="rank (1 = top)")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Type part of a song title, for example 'espresso'.")

# ---------------- SQL console ----------------
with tabs[4]:
    st.write("Write any Spark SQL against the view `spotify`. Columns: spotify_id, name, artists, daily_rank, "
             "daily_movement, weekly_movement, country, snapshot_date, popularity, is_explicit, duration_ms, "
             "album_name, album_release_date, danceability, energy, key, loudness, mode, speechiness, "
             "acousticness, instrumentalness, liveness, valence, tempo, time_signature.")
    sql = st.text_area("Spark SQL", height=170, value=(
        "SELECT country, COUNT(*) entries, ROUND(AVG(tempo),1) avg_tempo\n"
        "FROM spotify\nGROUP BY country\nORDER BY avg_tempo DESC\nLIMIT 20"))
    if st.button("Run query"):
        try:
            out = get_spark().sql(sql).limit(5000).toPandas()
            st.success(f"{len(out)} rows (max 5,000 shown)")
            st.dataframe(out, use_container_width=True)
            st.download_button("Download CSV", out.to_csv(index=False), "query_result.csv")
        except Exception as e:  # show the Spark error message to the user
            st.error(str(e).split("\n")[0])

# ---------------- Spark ML results ----------------
with tabs[5]:
    def res(name):
        files = glob.glob(f"{RESULTS}/{name}/part-*.csv")
        return pd.read_csv(files[0]) if files else None
    corr, prof = res("06_popularity_correlation"), res("07_cluster_profiles")
    if corr is None or prof is None:
        st.warning(f"Copy the Spark results to {RESULTS} first (see step 5 of the guide).")
    else:
        st.subheader("Correlation of audio features with popularity")
        st.plotly_chart(px.bar(corr.sort_values("corr_with_popularity"), x="corr_with_popularity",
                               y="feature", orientation="h"), use_container_width=True)
        st.subheader("KMeans clusters (MLlib)")
        st.dataframe(prof, use_container_width=True)
        z = prof.drop(columns=["cluster", "songs"])
        z = (z - z.mean()) / z.std(ddof=0)
        z.index = [f"Cluster {c}" for c in prof["cluster"]]
        st.plotly_chart(px.imshow(z, color_continuous_scale="RdYlGn", text_auto=".1f", aspect="auto"),
                        use_container_width=True)
