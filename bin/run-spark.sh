#!/usr/bin/env bash

source conf/config.sh
source basic.sh
setpath
ldfunc

JOB_LIST=(
#    "1M_PR_SPK"
#    "10M_PR_SPK"
#    "30M_PR_SPK"
#    "web-Google_PR_SPK"
#    "LiveJournal1_PR_SPK"
#    "com-friendster_PR_SPK"

#    "KDD04_KM_SPK"
#    "1M_KM_SPK_NEW"
#    "10M_KM_SPK_NEW"
#    "30M_KM_SPK_NEW"
    "60M_KM_SPK_NEW"

#    "2G_TERA_SPK"
#    "10G_TERA_SPK"
#    "20G_TERA_SPK"
#    "40G_TERA_SPK"
#    "80G_TERA_SPK"
#    "160G_TERA_SPK"

#    "2G_WC_SPK"
#    "10G_WC_SPK"
#    "20G_WC_SPK"
#    "40G_WC_SPK"
#    "80G_WC_SPK"
#    "160G_WC_SPK"

#    "2G_ST_SPK"
#    "10G_ST_SPK"
#    "20G_ST_SPK"
#    "40G_ST_SPK"
#    "80G_ST_SPK"
#    "160G_ST_SPK"
)

rm _job_list
for job in ${JOB_LIST[@]}; do
  echo $job >> _job_list
done

del_data /output
#sleep 60

export SPARKBENCH_PROPERTIES_FILES="$_TESTDIR/conf/bench-txt.conf" # for wc and st, pr
export SPARKBENCH_PROPERTIES_FILES="$_TESTDIR/conf/bench-seq.conf" # for terasort

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