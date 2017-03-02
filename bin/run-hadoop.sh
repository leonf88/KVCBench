#!/usr/bin/env bash

source conf/config.sh
source func/test-job-function-basic.sh
source func/test-job-function-hadoop.sh


do_pagerank_had()
{
    # use pegasus to run pagerank

    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
	let "NODES_NUMBER=2**${3}"
    REDS=$((${4} * ${HOSTS_NUM}))
	MAX_ITERS=${5}
    JOB_NAME="pagerank"

    cmd="${HADOOP_HOME}/bin/hadoop jar \
        ${HADOOP_BENCH_JAR} pegasus.pagerank.PagerankNaive \
        ${SOURCE_PATH} \
        ${TARGET_PATH} \
        ${NODES_NUMBER} \
        ${REDS} \
        ${MAX_ITERS} \
        nosym new"

    _do_hadoop_func

}



