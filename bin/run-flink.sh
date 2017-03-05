#!/usr/bin/env bash

source conf/config.sh
source basic.sh
setpath
ldfunc

JOB_LIST=(
#    "2G_TERA_FLK"
#    "10G_TERA_FLK"
#    "50G_TERA_FLK"
#    "100G_TERA_FLK"

#    "2G_WC_FLK"
    "10G_WC_FLK"
    "50G_WC_FLK"
    "100G_WC_FLK"

#    "2G_ST_FLK"
    "10G_ST_FLK"
    "50G_ST_FLK"
    "100G_ST_FLK"
)

rm _job_list
for job in ${JOB_LIST[@]}; do
  echo $job >> _job_list
done

del_data /output
sleep 60

export PDSH_RCMD_TYPE=ssh
bash $FLINK_HOME/bin/start-cluster.sh
bash ./runtest-test.sh
bash $FLINK_HOME/bin/stop-cluster.sh

if [ -d "results-flk" ]; then
   mv results/* results-flk
   rm -r results
else
   mv results/ results-flk
fi

exit
