#!/usr/bin/env bash

CUR_DIR=`dirname $0`
CUR_DIR=`cd "$CUR_DIR"; pwd`

export LOADED_CONFIG=true

MPI_D_SLAVES=$CUR_DIR/slaves

# mahout
MAHOUT_HOME="/home/nbtest/develop/mahout-distribution-0.9"

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