"""
Spotify Top Songs - Spark part
Run with:
  spark-submit --master local[*] --driver-memory 4g spark_spotify.py
(on a cluster use: --master yarn)
Reads the cleaned ORC table that Hive created (hive_spotify.sql) straight from the
Hive warehouse folder in HDFS, and writes result CSVs back to HDFS.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

OUT = "hdfs:///user/sejal/spotify/output/spark"   # 'hadoop' is replaced by your username via sed
CLEAN = "hdfs:///user/hive/warehouse/spotify_bda.db/spotify_clean"

spark = (SparkSession.builder.appName("SpotifyBDA")
         .config("spark.sql.shuffle.partitions", "50")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = spark.read.orc(CLEAN).cache()
print("Rows:", df.count())


def save(d, name):
    d.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/{name}")
    print("saved", name)


AUDIO = ["danceability", "energy", "loudness", "speechiness", "acousticness",
         "instrumentalness", "liveness", "valence", "tempo"]

# ---- 1. Data quality: null counts per column ----
nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
save(nulls, "01_null_counts")

# ---- 2. Every credited artist (explode collaborations) ----
artists = (df.withColumn("artist", F.explode(F.split("artists", r",\s*")))
             .groupBy("artist")
             .agg(F.count("*").alias("appearances"),
                  F.countDistinct("spotify_id").alias("unique_songs"),
                  F.countDistinct("country").alias("countries"),
                  F.round(F.avg("daily_rank"), 2).alias("avg_rank"))
             .orderBy(F.desc("appearances")))
save(artists.limit(50), "02_top_artists")

# ---- 3. Song reach and longevity ----
songs_reach = (df.groupBy("spotify_id", "name", "artists")
                 .agg(F.countDistinct("country").alias("countries"),
                      F.countDistinct("snapshot_date").alias("days_charted"),
                      F.min("daily_rank").alias("best_rank"),
                      F.round(F.avg("popularity"), 1).alias("avg_popularity"))
                 .orderBy(F.desc("countries"), F.desc("days_charted")))
save(songs_reach.limit(50), "03_song_reach")

# ---- 4. Country audio profile + explicit share ----
country_profile = (df.groupBy("country")
                     .agg(*[F.round(F.avg(c), 3).alias(c) for c in AUDIO],
                          F.round(100 * F.avg(F.col("is_explicit").cast("int")), 2).alias("explicit_pct"),
                          F.round(F.avg("popularity"), 1).alias("avg_popularity"))
                     .orderBy(F.desc("valence")))
save(country_profile, "04_country_profile")

# ---- 5. Monthly global trend ----
monthly = (df.filter(F.col("country") == "GLOBAL")
             .withColumn("month", F.date_format("snapshot_date", "yyyy-MM"))
             .groupBy("month")
             .agg(*[F.round(F.avg(c), 3).alias(c) for c in ["valence", "energy", "danceability", "tempo"]])
             .orderBy("month"))
save(monthly, "05_monthly_global_trend")

# ---- 6. Correlation of each audio feature with popularity ----
corr_rows = [(c, float(df.stat.corr(c, "popularity"))) for c in AUDIO + ["duration_ms"]]
corr = spark.createDataFrame(corr_rows, ["feature", "corr_with_popularity"]) \
            .orderBy(F.desc(F.abs(F.col("corr_with_popularity"))))
save(corr, "06_popularity_correlation")

# ---- 7. MLlib: cluster songs by audio features (KMeans) ----
songs = (df.groupBy("spotify_id", "name", "artists")
           .agg(*[F.avg(c).alias(c) for c in AUDIO]).dropna())
vec = VectorAssembler(inputCols=AUDIO, outputCol="raw").transform(songs)
scaled = StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True) \
    .fit(vec).transform(vec)

best_k, best_score, best_model = None, -1, None
for k in range(3, 8):
    model = KMeans(k=k, seed=42, featuresCol="features").fit(scaled)
    score = ClusteringEvaluator(featuresCol="features").evaluate(model.transform(scaled))
    print(f"k={k} silhouette={score:.4f}")
    if score > best_score:
        best_k, best_score, best_model = k, score, model
print("Best k:", best_k)

clustered = best_model.transform(scaled).withColumnRenamed("prediction", "cluster")
profile = (clustered.groupBy("cluster")
                    .agg(F.count("*").alias("songs"),
                         *[F.round(F.avg(c), 3).alias(c) for c in AUDIO])
                    .orderBy("cluster"))
save(profile, "07_cluster_profiles")
save(clustered.select("spotify_id", "name", "artists", "cluster"), "07_song_clusters")

spark.stop()
