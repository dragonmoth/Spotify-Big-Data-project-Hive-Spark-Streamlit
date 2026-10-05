Spotify Top 50: Big Data Analytics (Hive + Spark)
Analysis of about 2.1 million daily Spotify top-50 chart entries (73 charts, Oct 2023 to Jun 2025)
using HDFS, Hive, Spark (SQL + MLlib KMeans) and a Streamlit dashboard.
Pipeline: CSV -> HDFS -> Hive (raw table, then cleaned ORC table) -> Spark analysis + KMeans -> Streamlit app
Files
File	Purpose
`setup_wsl.sh`	Installs Java 8, Hadoop 3.3.6, Hive 3.1.3, Spark 3.5.1 in WSL2 Ubuntu
`hive_spotify.sql`	Creates the raw and cleaned tables and runs 10 analysis queries
`spark_spotify.py`	Spark analysis (artists, reach, country profile, trends, correlation) and KMeans clustering
`app.py`	Streamlit dashboard running live Spark SQL on the Hive table
`make_charts.py`	Saves report charts as PNG from the Spark result CSVs
`hive_output.txt`, `spark_output.txt`	Saved outputs of the runs
`results/`, `charts/`	Spark result CSVs and chart images
The dataset (`universal_top_spotify_songs.csv`, about 498 MB) is not included.
Requirements
Windows 10/11 with WSL2 (Ubuntu 22.04 or 24.04), 8 GB RAM or more. Set `memory=5GB` in `C:\Users\<you>\.wslconfig`.
How to run (inside Ubuntu)
```bash
cd /mnt/c/bda
sed -i 's/\r$//' setup_wsl.sh && bash setup_wsl.sh     # once; then reopen Ubuntu

sudo service ssh start && start-dfs.sh && hdfs dfsadmin -safemode wait

sed -i "s#/user/hadoop#/user/$USER#g" hive_spotify.sql spark_spotify.py
hdfs dfs -mkdir -p /user/$USER/spotify/raw /user/hive/warehouse /tmp
hdfs dfs -chmod -R 777 /user/hive /tmp
hdfs dfs -put universal_top_spotify_songs.csv /user/$USER/spotify/raw/

cd ~/bigdata
export HADOOP_HEAPSIZE=2048
hive --hiveconf mapreduce.input.fileinputformat.split.maxsize=67108864 -f /mnt/c/bda/hive_spotify.sql

sudo apt install -y python3-numpy
spark-submit --master "local[*]" --driver-memory 3g /mnt/c/bda/spark_spotify.py
hdfs dfs -get /user/$USER/spotify/output/spark /mnt/c/bda/results
```
Dashboard
```bash
sudo apt install -y python3-venv
python3 -m venv ~/spotify_env --system-site-packages && source ~/spotify_env/bin/activate
pip install streamlit plotly pandas pyarrow pyspark==3.5.1
streamlit run /mnt/c/bda/app.py --server.headless true     # open http://localhost:8501
```
Findings (from the Spark results)
Audio features explain very little of popularity. Every correlation is weak; the strongest is speechiness (r = -0.15).
KMeans (k = 3, silhouette 0.26) separates upbeat mainstream, mellow/acoustic and speech-heavy songs. The groups overlap.
