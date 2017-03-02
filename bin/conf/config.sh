#!/usr/bin/env bash

# import the basic before each evaluation script for proper settings
# set the basic properties for evaluation

export DEFINE_ONERROR=true
onerror()
{
  local _REASON="$1"
  echo "=== Error ===" | tee -a "$LOG_NAME"
  echo -e "Info: $_REASON." | tee -a "$LOG_NAME"
  echo "[FAIL] MPI-D Test $NB_DATE: $_REASON"
  exit 1
}


setpath()
{
  _TESTDIR=${1:-`pwd`}
  _TEST_LOG_DIR=${2:-$_TESTDIR/logs}
  NB_DATE=`date +%Y%m%d-%s`
  LOG_NAME="$_TEST_LOG_DIR/app_${NB_DATE}.log"
  REPORT_NAME="$_TEST_LOG_DIR/app_report_${NB_DATE}.log"
  export _TESTDIR LOG_NAME REPORT_NAME
  [ ! -d "$_TEST_LOG_DIR" ] && mkdir -p $_TEST_LOG_DIR
}

ldfunc()
{
  FUNC_DIR="func"
  if [ -d "$FUNC_DIR" ]; then
    for f in `ls $FUNC_DIR`; do
      source "$FUNC_DIR/$f"
    done
  else
    echo "No $FUNC_DIR directory found."
    exit 1
  fi
}

export LOADED_CONFIG=true

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