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

#    "KDD04_KM_HAD"
#    "1M_KM_HAD_NEW"
#    "10M_KM_HAD_NEW"
#    "30M_KM_HAD_NEW"
    "60M_KM_HAD_NEW"

#    "2G_TERA_HAD"
#    "10G_TERA_HAD"
#    "20G_TERA_HAD"
#    "40G_TERA_HAD"
#    "80G_TERA_HAD"
#    "160G_TERA_HAD"

#    "2G_WC_HAD"
#    "10G_WC_HAD"
#    "20G_WC_HAD"
#    "40G_WC_HAD"
#    "80G_WC_HAD"
#    "160G_WC_HAD"

#    "2G_ST_HAD"
#    "10G_ST_HAD"
#    "20G_ST_HAD"
#    "40G_ST_HAD"
#    "80G_ST_HAD"
#    "160G_ST_HAD"
)

rm _job_list
for job in ${JOB_LIST[@]}; do
  echo $job >> _job_list
done

del_data /output
#sleep 60

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
