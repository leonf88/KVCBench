#!/usr/bin/env bash

CUR_DIR=`dirname $0`
CUR_DIR=`cd "$CUR_DIR"; pwd`

export LOADED_CONFIG=true

MPI_D_SLAVES=$CUR_DIR/slaves

HDFS_MASTER="hdfs://172.22.1.21:9003"

# mahout
MAHOUT_HOME="/home/lf/workplace/BenchScripts/frameworks/apache-mahout-distribution-0.12.2"

# hadoop-1
# HADOOP_HOME="/home/nbtest/develop/hadoop-1.2.1"
# HADOOP_CONF_DIR="${HADOOP_HOME}/conf"
# HDFS_CONF_CORE="${HADOOP_HOME}/conf/core-site.xml" # needed by datampi
# HADOOP_EXAMPLE_JAR="${HADOOP_HOME}/example.jar"
# HADOOP_BENCH_JAR="$_TESTDIR/lib/hadoop-example.jar"


# hadoop-2
HADOOP_HOME="/home/lf/workplace/BenchScripts/frameworks/hadoop-2.7.3"
HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
HDFS_CONF_CORE="${HADOOP_CONF_DIR}/core-site.xml" # needed by datampi
HADOOP_EXAMPLE_JAR="/home/lf/workplace/BenchScripts/frameworks/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar"
HADOOP_BENCH_JAR="/home/lf/workplace/BenchScripts/hadoop-benchmarks/target/hadoop-benchmarks-1.0-SNAPSHOT.jar"

# spark-1.6
SPARK_HOME="/home/lf/workplace/BenchScripts/frameworks/spark-1.6.2-bin-hadoop2.6"
SPARK_MASTER="spark://172.22.1.21:7077"
MAX_CORES=28
EXEC_MEM=12g
SPARK_BENCH_JAR="/home/lf/workplace/BenchScripts/spark-benchmarks/target/spark-benchmarks-1.0-SNAPSHOT.jar"
SPARK_BENCH_JAR="/home/lf/workplace/BenchScripts/spark-benchmarks/target/spark-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar"
SPARK_PROP_CONF="/home/lf/workplace/BenchScripts/frameworks/spark-1.6.2-bin-hadoop2.6/conf/spark-defaults.conf"

# flink-1.1.2
FLINK_HOME="/home/lf/workplace/BenchScripts/frameworks/flink-1.1.2"
FLINK_BENCH_JAR="/home/lf/workplace/BenchScripts/flink-benchmarks/target/flink-benchmarks-1.0-SNAPSHOT.jar"



