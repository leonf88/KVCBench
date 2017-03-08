#!/usr/bin/env bash

SCALA_BIN_VERSION=${SCALA_BIN_VERSION:-"2.11"}
SCALA_SUB_VERSION=${SCALA_SUB_VERSION:-"8"}
FLINK_VERSION=${FLINK_VERSION:-"1.1.2"}
SPARK_VERSION=${SPARK_VERSION:-"1.6.2"}
HADOOP_VERSION=${HADOOP_VERSION:-"2.7.3"}
MAHOUT_VERSION=${MAHOUT_VERSION:-"0.12.2"}
MPICH_VERSION=${MPICH_VERSION:-"3.1.4"}

HADOOP_DIR="hadoop-$HADOOP_VERSION"
FLINK_DIR="flink-$FLINK_VERSION"
SPARK_DIR="spark-$SPARK_VERSION-bin-hadoop2.6"
MAHOUT_DIR="apache-mahout-distribution-${MAHOUT_VERSION}"
MPICH_DIR="mpich-${MPICH_VERSION}"

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

if [[ $# == 1 ]];
then
  #Get one of the closet apache mirrors
  APACHE_MIRROR=$(curl 'https://www.apache.org/dyn/closer.lua' |   grep -o '<strong>[^<]*</strong>' |   sed 's/<[^>]*>//g' |   head -1)

  # Fetch Hadoop
  HADOOP_FILE="$HADOOP_DIR.tar.gz"
  fetch_untar_file "$HADOOP_FILE" "$APACHE_MIRROR/hadoop/common/hadoop-$HADOOP_VERSION/$HADOOP_FILE"

  # Fetch Flink
  FLINK_FILE="$FLINK_DIR-bin-hadoop27-scala_${SCALA_BIN_VERSION}.tgz"
#  fetch_untar_file "$FLINK_FILE" "$APACHE_MIRROR/flink/flink-$FLINK_VERSION/$FLINK_FILE"
  fetch_untar_file "$FLINK_FILE" "http://archive.apache.org/dist/flink/${FLINK_DIR}/${FLINK_FILE}"

  # Fetch Spark
  SPARK_FILE="$SPARK_DIR.tgz"
  fetch_untar_file "$SPARK_FILE" "$APACHE_MIRROR/spark/spark-$SPARK_VERSION/$SPARK_FILE"

  # Fetch Mahout
  MAHOUT_FILE="$MAHOUT_DIR.tar.gz"
  fetch_untar_file "$MAHOUT_FILE" "$APACHE_MIRROR/mahout/$MAHOUT_VERSION/$MAHOUT_FILE"

  # MPICH
  MPICH_FILE="$MPICH_DIR.tar.gz"
  fetch_untar_file "$MPICH_FILE" "http://www.mpich.org/static/downloads/${MPICH_VERSION}/${MPICH_FILE}"
else
  # use source file to create the environment variables
  PATH=`pwd`/$HADOOP_DIR/bin:$PATH
  PATH=`pwd`/$FLINK_DIR/bin:$PATH
  PATH=`pwd`/$SPARK_DIR/bin:$PATH
  export PATH
  echo $PATH
fi


