#!/usr/bin/env bash


HADOOP_BIN=
GEN_JAR=

OPTION="-t pagerank \
        -b ${PAGERANK_BASE_HDFS} \
        -n Input \
        -m ${NUM_MAPS} \
        -r ${NUM_REDS} \
        -p ${PAGES} \
        -pbalance -pbalance \
        -o text"

$HADOOP_BIN ${GEN_JAR} HiBench.DataGen ${OPTION}

