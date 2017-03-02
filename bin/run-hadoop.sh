#!/usr/bin/env bash

source conf/config.sh
source func/test-job-function-basic.sh
source func/test-job-function-hadoop.sh

do_pagerank_had /data/pagerank/30M /hadoop/pagerank/output/30M 25 28 10


JOB_LIST=(
  "30M_PR_HAD_1I"
)

rm _job_list
for job in ${JOB_LIST[@]}; do
  echo $job >> _job_list
done
