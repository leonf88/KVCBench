#!/usr/bin/env bash

source conf/config.sh
source basic.sh
setpath
ldfunc

JOB_LIST=(
#    "1M_PR_DM_TEST"
#    "1M_PR_DM"
#    "10M_PR_DM"
#    "30M_PR_DM"
#    "web-Google_PR_DM"
#    "LiveJournal1_PR_DM"
#    "com-friendster_PR_DM"

#    "KDD04_KM_DM_TEST"
#    "KDD04_KM_DM"
#    "1M_KM_DM"
#    "10M_KM_DM"
#    "30M_KM_DM"
#    "60M_KM_DM_NEW"

#    "2G_TERA_DM"
#    "10G_TERA_DM"
#    "20G_TERA_DM"
#    "40G_TERA_DM"
#    "80G_TERA_DM"
#    "160G_TERA_DM"

#    "0_SLEEP_DM"
#    "2G_WC_DM"
#    "10G_WC_DM"
#    "20G_WC_DM"
#    "40G_WC_DM"
#    "80G_WC_DM"
#    "160G_WC_DM"

#    "2G_ST_DM"
#    "10G_ST_DM"
#    "20G_ST_DM"
#    "40G_ST_DM"
#    "80G_ST_DM"
#    "160G_ST_DM"
)

rm _job_list
for job in ${JOB_LIST[@]}; do
  echo $job >> _job_list
done

#del_data /output
#sleep 60

bash ./runtest-test.sh

if [ -d "results-dm" ]; then
   mv results/* results-dm
   rm -r results
else
   mv results/ results-dm
fi

exit