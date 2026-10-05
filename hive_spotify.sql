-- =====================================================
-- Spotify Top Songs - Hive part
-- Run with:  hive -f hive_spotify.sql
-- Change /user/sejal/... below if your HDFS user is different
-- =====================================================

CREATE DATABASE IF NOT EXISTS spotify_bda;
USE spotify_bda;

-- ---------- 1. RAW external table (everything as STRING) ----------
-- The CSV has every field quoted, so we use OpenCSVSerde.
DROP TABLE IF EXISTS raw_spotify;
CREATE EXTERNAL TABLE raw_spotify (
  spotify_id STRING, name STRING, artists STRING,
  daily_rank STRING, daily_movement STRING, weekly_movement STRING,
  country STRING, snapshot_date STRING, popularity STRING,
  is_explicit STRING, duration_ms STRING, album_name STRING,
  album_release_date STRING, danceability STRING, energy STRING,
  `key` STRING, loudness STRING, mode STRING, speechiness STRING,
  acousticness STRING, instrumentalness STRING, liveness STRING,
  valence STRING, tempo STRING, time_signature STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE
LOCATION '/user/sejal/spotify/raw'
TBLPROPERTIES ("skip.header.line.count" = "1");

-- sanity check: expect 2,110,316 rows
SELECT COUNT(*) FROM raw_spotify;

-- ---------- 2. CLEAN table (typed, ORC) ----------
DROP TABLE IF EXISTS spotify_clean;
CREATE TABLE spotify_clean STORED AS ORC AS
SELECT
  spotify_id,
  name,
  artists,
  CAST(daily_rank AS INT)        AS daily_rank,
  CAST(daily_movement AS INT)    AS daily_movement,
  CAST(weekly_movement AS INT)   AS weekly_movement,
  CASE WHEN country = '' OR country IS NULL THEN 'GLOBAL' ELSE country END AS country,
  CAST(snapshot_date AS DATE)    AS snapshot_date,
  CAST(popularity AS INT)        AS popularity,
  (is_explicit = 'True')         AS is_explicit,
  CAST(duration_ms AS INT)       AS duration_ms,
  album_name,
  CASE WHEN LENGTH(album_release_date) = 10
       THEN CAST(album_release_date AS DATE) END AS album_release_date,
  CAST(danceability AS DOUBLE)     AS danceability,
  CAST(energy AS DOUBLE)           AS energy,
  CAST(`key` AS INT)               AS `key`,
  CAST(loudness AS DOUBLE)         AS loudness,
  CAST(mode AS INT)                AS mode,
  CAST(speechiness AS DOUBLE)      AS speechiness,
  CAST(acousticness AS DOUBLE)     AS acousticness,
  CAST(instrumentalness AS DOUBLE) AS instrumentalness,
  CAST(liveness AS DOUBLE)         AS liveness,
  CAST(valence AS DOUBLE)          AS valence,
  CAST(tempo AS DOUBLE)            AS tempo,
  CAST(time_signature AS INT)      AS time_signature
FROM raw_spotify
WHERE spotify_id IS NOT NULL AND spotify_id <> '';

SELECT COUNT(*) FROM spotify_clean;

-- =====================================================
-- ANALYSIS QUERIES
-- =====================================================

-- Q1. Date range and number of countries
SELECT MIN(snapshot_date) AS first_day, MAX(snapshot_date) AS last_day,
       COUNT(DISTINCT country) AS countries, COUNT(DISTINCT spotify_id) AS unique_songs
FROM spotify_clean;

-- Q2. Songs charting in the most countries (global reach)
SELECT name, artists, COUNT(DISTINCT country) AS countries_charted,
       COUNT(*) AS total_chart_days
FROM spotify_clean
WHERE country <> 'GLOBAL'
GROUP BY spotify_id, name, artists
ORDER BY countries_charted DESC, total_chart_days DESC
LIMIT 20;

-- Q3. Longest-lasting songs in the GLOBAL top 50
SELECT name, artists, COUNT(DISTINCT snapshot_date) AS days_in_chart,
       ROUND(AVG(daily_rank), 2) AS avg_rank, MIN(daily_rank) AS best_rank
FROM spotify_clean
WHERE country = 'GLOBAL'
GROUP BY spotify_id, name, artists
ORDER BY days_in_chart DESC
LIMIT 20;

-- Q4. Explicit-content share per country
SELECT country,
       ROUND(100 * SUM(CASE WHEN is_explicit THEN 1 ELSE 0 END) / COUNT(*), 2) AS explicit_pct
FROM spotify_clean
GROUP BY country
ORDER BY explicit_pct DESC;

-- Q5. Average audio profile per country
SELECT country,
       ROUND(AVG(danceability), 3) AS danceability, ROUND(AVG(energy), 3) AS energy,
       ROUND(AVG(valence), 3) AS valence, ROUND(AVG(tempo), 1) AS tempo,
       ROUND(AVG(acousticness), 3) AS acousticness
FROM spotify_clean
GROUP BY country
ORDER BY valence DESC;

-- Q6. Monthly trend of the global mood (valence / energy / danceability)
SELECT DATE_FORMAT(snapshot_date, 'yyyy-MM') AS month,
       ROUND(AVG(valence), 3) AS valence, ROUND(AVG(energy), 3) AS energy,
       ROUND(AVG(danceability), 3) AS danceability
FROM spotify_clean
WHERE country = 'GLOBAL'
GROUP BY DATE_FORMAT(snapshot_date, 'yyyy-MM')
ORDER BY month;

-- Q7. Artists with the most chart appearances (first-listed artist only)
SELECT TRIM(SPLIT(artists, ',')[0]) AS main_artist,
       COUNT(*) AS appearances, COUNT(DISTINCT spotify_id) AS unique_songs
FROM spotify_clean
GROUP BY TRIM(SPLIT(artists, ',')[0])
ORDER BY appearances DESC
LIMIT 20;

-- Q8. Every artist credited (LATERAL VIEW explode handles collaborations)
SELECT TRIM(a) AS artist, COUNT(*) AS appearances
FROM spotify_clean
LATERAL VIEW EXPLODE(SPLIT(artists, ',')) t AS a
GROUP BY TRIM(a)
ORDER BY appearances DESC
LIMIT 20;

-- Q9. Does song age matter? Avg days between release and chart date, per country
SELECT country,
       ROUND(AVG(DATEDIFF(snapshot_date, album_release_date)), 0) AS avg_song_age_days
FROM spotify_clean
WHERE album_release_date IS NOT NULL
GROUP BY country
ORDER BY avg_song_age_days;

-- Q10. Biggest single-day climbers
SELECT snapshot_date, country, name, artists, daily_rank, daily_movement
FROM spotify_clean
ORDER BY daily_movement DESC
LIMIT 20;

-- To export a result to HDFS as CSV, for example Q5:
-- INSERT OVERWRITE DIRECTORY '/user/sejal/spotify/output/hive_q5'
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
-- SELECT ... ;
