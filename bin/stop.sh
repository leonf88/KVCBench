#!/usr/bin/env bash

curDir=$(cd `dirname $0`;pwd)

# Read execution nodes
NODES=()
cnt=0
for n in `cat conf/slaves`;do
  NODES[cnt]=$n
  cnt=$((cnt+1))
done

keyWords=(perf dstat)
for host in ${NODES[@]}
do
    for key in ${keyWords[@]}
    do
        for pid in `ssh ${host} "ps aux | grep ${key}" | grep -v grep | awk '{print $2}'`
        do
			echo kill $host $key
            ssh ${host} "sudo kill -2 $pid"
        done
    done
done

source conf/config.sh
source basic.sh
setpath
ldfunc

bash $SPARK_HOME/sbin/stop-all.sh

bash $HADOOP_HOME/sbin/stop-yarn.sh

export PDSH_RCMD_TYPE=ssh
bash $FLINK_HOME/bin/stop-cluster.sh

