#!/usr/bin/env bash

source conf/config.sh
source basic.sh
setpath
ldfunc

JOB_LIST=(
#  "web-Google_HAD"
#  "LiveJournal1_HAD"
#  "com-friendster_HAD"
#  "1M_PR_HAD"
#  "10M_PR_HAD"
#  "30M_PR_HAD"

#    "1M_KM_HAD_NEW"
#    "10M_KM_HAD_NEW"
#    "30M_KM_HAD_NEW"
#    "60M_KM_HAD_NEW"

#    "10G_TERA_HAD"
#    "50G_TERA_HAD"
#    "100G_TERA_HAD"

#    "2G_WC_HAD"
    "10G_WC_HAD"
    "50G_WC_HAD"
    "100G_WC_HAD"

#    "2G_ST_HAD"
    "10G_ST_HAD"
    "50G_ST_HAD"
    "100G_ST_HAD"
)

rm _job_list
for job in ${JOB_LIST[@]}; do
  echo $job >> _job_list
done

del_data /output
sleep 60

bash $HADOOP_HOME/sbin/start-yarn.sh
bash ./runtest-test.sh
bash $HADOOP_HOME/sbin/stop-yarn.sh

if [ -d "results-had" ]; then
   mv results/* results-had
   rm -r results
else
   mv results/ results-had
fi

exit
