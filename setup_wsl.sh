#!/usr/bin/env bash
# One-time setup: Hadoop 3.3.6 + Hive 3.1.3 + Spark 3.5.1 on WSL2 Ubuntu (pseudo-distributed, single machine)
# Run inside Ubuntu:  bash setup_wsl.sh
set -e

BASE="$HOME/bigdata"
JAVA8="/usr/lib/jvm/java-8-openjdk-amd64"

echo "== 1. Packages =="
sudo apt update
sudo apt install -y openjdk-8-jdk openssh-server wget unzip

mkdir -p "$BASE" && cd "$BASE"

echo "== 2. Download (about 1 GB total) =="
wget -c https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
wget -c https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
wget -c https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz

echo "== 3. Extract =="
[ -d hadoop-3.3.6 ] || tar -xzf hadoop-3.3.6.tar.gz
[ -d apache-hive-3.1.3-bin ] || tar -xzf apache-hive-3.1.3-bin.tar.gz
[ -d spark-3.5.1-bin-hadoop3 ] || tar -xzf spark-3.5.1-bin-hadoop3.tgz
ln -sfn hadoop-3.3.6 hadoop
ln -sfn apache-hive-3.1.3-bin hive
ln -sfn spark-3.5.1-bin-hadoop3 spark

echo "== 4. Environment variables =="
if ! grep -q "BIGDATA_ENV" ~/.bashrc; then
cat >> ~/.bashrc <<EOF

# BIGDATA_ENV
export JAVA_HOME=$JAVA8
export HADOOP_HOME=$BASE/hadoop
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export HIVE_HOME=$BASE/hive
export SPARK_HOME=$BASE/spark
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$HIVE_HOME/bin:\$SPARK_HOME/bin
EOF
fi
export JAVA_HOME=$JAVA8 HADOOP_HOME=$BASE/hadoop HADOOP_CONF_DIR=$BASE/hadoop/etc/hadoop
export HIVE_HOME=$BASE/hive SPARK_HOME=$BASE/spark
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$SPARK_HOME/bin

echo "== 5. Passwordless SSH to localhost (needed by start-dfs.sh) =="
mkdir -p ~/.ssh && chmod 700 ~/.ssh
[ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
grep -qF "$(cat ~/.ssh/id_rsa.pub)" ~/.ssh/authorized_keys 2>/dev/null || cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
sudo service ssh start
ssh-keyscan -H localhost >> ~/.ssh/known_hosts 2>/dev/null || true

echo "== 6. Hadoop config =="
echo "export JAVA_HOME=$JAVA8" >> "$HADOOP_CONF_DIR/hadoop-env.sh"
cat > "$HADOOP_CONF_DIR/core-site.xml" <<EOF
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
</configuration>
EOF
cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<EOF
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://$BASE/hdfs/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://$BASE/hdfs/datanode</value></property>
</configuration>
EOF
mkdir -p "$BASE/hdfs/namenode" "$BASE/hdfs/datanode"
[ -d "$BASE/hdfs/namenode/current" ] || hdfs namenode -format -force

echo "== 7. Hive config =="
# Hive 3.1.3 ships an old Guava that clashes with Hadoop 3.3.x; swap it
rm -f "$HIVE_HOME"/lib/guava-19.0.jar
cp "$HADOOP_HOME"/share/hadoop/hdfs/lib/guava-27.0-jre.jar "$HIVE_HOME/lib/"
cat > "$HIVE_HOME/conf/hive-site.xml" <<EOF
<configuration>
  <property><name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=$BASE/metastore_db;create=true</value></property>
  <property><name>javax.jdo.option.ConnectionDriverName</name><value>org.apache.derby.jdbc.EmbeddedDriver</value></property>
  <property><name>hive.metastore.warehouse.dir</name><value>/user/hive/warehouse</value></property>
  <property><name>hive.metastore.schema.verification</name><value>false</value></property>
</configuration>
EOF
[ -d "$BASE/metastore_db" ] || schematool -dbType derby -initSchema

echo
echo "SETUP DONE. Close and reopen the Ubuntu terminal, then follow the next steps."
