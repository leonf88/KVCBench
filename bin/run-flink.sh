#!/usr/bin/env bash

source conf/config.sh
source basic.sh
setpath
ldfunc

JOB_LIST=(
#    "2G_TERA_FLK"
#    "10G_TERA_FLK"
#    "20G_TERA_FLK"
#    "40G_TERA_FLK"
#    "80G_TERA_FLK"
#    "160G_TERA_FLK"

#    "2G_WC_FLK"
#    "10G_WC_FLK"
#    "20G_WC_FLK"
#    "40G_WC_FLK"
#    "80G_WC_FLK"
#    "160G_WC_FLK"

#    "2G_ST_FLK"
#    "10G_ST_FLK"
#    "20G_ST_FLK"
#    "40G_ST_FLK"
#    "80G_ST_FLK"
#    "160G_ST_FLK"
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
