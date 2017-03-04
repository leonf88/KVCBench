#!/usr/bin/env bash

source conf/config.sh

JOB_LIST=(
#  "web-Google_HAD"
#  "LiveJournal1_HAD"
#  "com-friendster_HAD"
#  "1M_PR_HAD"
#  "10M_PR_HAD"
#  "30M_PR_HAD"
#    "1M_KM_HAD_NEW"
#    "10M_KM_HAD_NEW"
    "30M_KM_HAD_NEW"
)

rm _job_list
for job in ${JOB_LIST[@]}; do
  echo $job >> _job_list
done

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
