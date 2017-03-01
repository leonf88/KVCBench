#!/usr/bin/env bash

SCALA_BIN_VERSION=${SCALA_BIN_VERSION:-"2.11"}
SCALA_SUB_VERSION=${SCALA_SUB_VERSION:-"8"}
FLINK_VERSION=${FLINK_VERSION:-"1.1.2"}
SPARK_VERSION=${SPARK_VERSION:-"1.6.2"}
HADOOP_VERSION=${HADOOP_VERSION:-"2.7.3"}

HADOOP_DIR="hadoop-$HADOOP_VERSION"
FLINK_DIR="flink-$FLINK_VERSION"
SPARK_DIR="spark-$SPARK_VERSION-bin-hadoop2.6"

#Get one of the closet apache mirrors
APACHE_MIRROR=$(curl 'https://www.apache.org/dyn/closer.cgi' |   grep -o '<strong>[^<]*</strong>' |   sed 's/<[^>]*>//g' |   head -1)

fetch_untar_file() {
  local FILE="download-cache/$1"
  local URL=$2
  if [[ -e "$FILE" ]];
  then
    echo "Using cached File $FILE"
  else
    mkdir -p download-cache/
    WGET=`whereis wget`
    CURL=`whereis curl`
    if [ -n "$WGET" ];
    then
      wget -O "$FILE" "$URL"
    elif [ -n "$CURL" ];
    then
      curl -o "$FILE" "$URL"
    else
      echo "Please install curl or wget to continue.";
      exit 1
    fi
  fi
  tar -xzvf "$FILE"
}

# Fetch Hadoop
HADOOP_FILE="$HADOOP_DIR.tgz"
fetch_untar_file "$HADOOP_FILE" "$APACHE_MIRROR/hadoop/hadoop-$HADOOP_VERSION/$HADOOP_FILE"

# Fetch Flink
FLINK_FILE="$FLINK_DIR-bin-hadoop27-scala_${SCALA_BIN_VERSION}.tgz"
fetch_untar_file "$FLINK_FILE" "$APACHE_MIRROR/flink/flink-$FLINK_VERSION/$FLINK_FILE"

# Fetch Spark
SPARK_FILE="$SPARK_DIR.tgz"
fetch_untar_file "$SPARK_FILE" "$APACHE_MIRROR/spark/spark-$SPARK_VERSION/$SPARK_FILE"

