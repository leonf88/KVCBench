#!/usr/bin/env bash

source conf/config.sh

JOB_LIST=(
#  "web-Google_HAD"
#  "LiveJournal1_HAD"
#  "com-friendster"
  "1M_PR_HAD"
  "10M_PR_HAD"
  "30M_PR_HAD"
)

rm _job_list
for job in ${JOB_LIST[@]}; do
  echo $job >> _job_list
done

bash ./runtest-test.sh
