#!/usr/bin/env bash

source conf/config.sh

JOB_LIST=(
  "30M_PR_HAD_1I"
)

rm _job_list
for job in ${JOB_LIST[@]}; do
  echo $job >> _job_list
done

bash ./runtest-test.sh
