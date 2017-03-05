#!/usr/bin/env bash

source conf/config.sh
source basic.sh
setpath
ldfunc

JOB_LIST=(
#    "1M_PR_SPK"
#    "10M_PR_SPK"
#    "30M_PR_SPK"
#    "web-Google_SPK"
#    "LiveJournal1_SPK"
#    "com-friendster_SPK"

#    "1M_KM_SPK_NEW"
#    "10M_KM_SPK_NEW"
#    "30M_KM_SPK_NEW"

#    "10G_TERA_SPK"
    "50G_TERA_SPK"
#    "100G_TERA_SPK"
)

rm _job_list
for job in ${JOB_LIST[@]}; do
  echo $job >> _job_list
done

del_data /output/tera
sleep 60

bash $SPARK_HOME/sbin/start-all.sh
bash ./runtest-test.sh
bash $SPARK_HOME/sbin/stop-all.sh

if [ -d "results-spk" ]; then
   mv results/* results-spk
   rm -r results
else
   mv results/ results-spk
fi

exit