#!/usr/bin/env bash

# current_dir=`dirname "$0"`
# current_dir=`cd "$current_dir"; pwd`
# root_dir=${current_dir}/../../../../..
# workload_config=${root_dir}/conf/workloads/websearch/pagerank.conf
# . "${root_dir}/bin/functions/load-bench-config.sh"
#
# enter_bench HadoopPreparePagerank ${workload_config} ${current_dir}
# show_bannar start
#
# rmr-hdfs $INPUT_HDFS || true
START_TIME=`timestamp`

OPTION="-t pagerank \
        -b ${PAGERANK_BASE_HDFS} \
        -n Input \
        -m ${NUM_MAPS} \
        -r ${NUM_REDS} \
        -p ${PAGES} \
        -pbalance -pbalance \
        -o text"

run-hadoop-job ${DATATOOLS} HiBench.DataGen ${OPTION}

END_TIME=`timestamp`

show_bannar finish
leave_bench
