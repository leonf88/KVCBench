#!/bin/bash

flink_LAUNCH="${flink_HOME}/bin/flink-class"
file="$_TESTDIR/func/test-job-function-basic.sh"
if [ -f "$file" ]; then
    . "$file"
else
    echo "No $file found."
    exit 1
fi

[ "$IMPORT_BASIC" != "true" ] && echo "Please import the basic job function script 'test-job-function-basic.sh'" && exit 1

##  flink jobs
_do_flink_func()
{
    # need set TARGET_PATH, SOURCE_PATH, JOB_NAME, HADOOP_HOME, LOG_NAME, REPORT_NAME
    del_data ${TARGET_PATH}
    isSrcExist=`${HADOOP_HOME}/bin/hadoop fs -ls ${SOURCE_PATH}`
    if [ "$isSrcExist" = "" ]
    then
        echo "[FAIL] FLINK ${JOB_NAME} `basename $SOURCE_PATH`. Doesn't have $SOURCE_PATH" | tee -a $REPORT_NAME
        return 1
    fi

    i=1
    for((;i!=0;i--)){
        [ ! -f "${LOG_NAME}" ] && touch ${LOG_NAME}
        startLine=`wc -l ${LOG_NAME} | awk '{print $1}'`
        t1=`date +%s`
        echo $cmd | tee -a ${LOG_NAME} && (${cmd}) 2>&1 | tee -a ${LOG_NAME}
        t2=`date +%s`

        flink_stat=`check_log $startLine`
        if [ "$flink_stat" = "0" ] 
        then 
            break 
        fi
        del_data ${TARGET_PATH}
        sleep 15
    }
    if [ "$flink_stat" = "0" ]
    then
        echo "[OK] Flink ${JOB_NAME} `basename $SOURCE_PATH` cost $((t2-t1)) sec" | tee -a $REPORT_NAME
    else
        echo "[FAIL] Flink ${JOB_NAME} `basename $SOURCE_PATH`" | tee -a $REPORT_NAME
        del_data ${TARGET_PATH}
    fi
    del_data "${SOURCE_PATH}/_*"

    startLine=0
}

do_text_sort()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    JOB_NAME="textst"

    cmd="${flink_LAUNCH} scala.Sort \
        $flink_MASTER \
        ${HDFS_MASTER}/${SOURCE_PATH} \
        ${HDFS_MASTER}/${TARGET_PATH} ${MAX_CORES} ${EXEC_MEM}"

    _do_flink_func
}

do_text_wc()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    JOB_NAME="wordcount"

    cmd="${flink_LAUNCH} scala.WordCount \
        $flink_MASTER \
        ${HDFS_MASTER}/${SOURCE_PATH} \
        ${HDFS_MASTER}/${TARGET_PATH} ${MAX_CORES} ${EXEC_MEM}"
    cmd="${FLINK_HOME}/bin/flink \
        run \
        -c microbench.WordCount \
        ${FLINK_BENCH_JAR} \
        ${HDFS_MASTER} \
        ${SOURCE_PATH} \
        ${TARGET_PATH} \
        ${REDS}"

    _do_flink_func
}

do_text_grep()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    REGEX=${3}
    GROUP=${4}
    JOB_NAME="grep"

    cmd="${flink_LAUNCH} scala.Grep \
        ${flink_MASTER} \
        ${HDFS_MASTER}/${SOURCE_PATH} \
        ${HDFS_MASTER}/${TARGET_PATH} \
        ${REGEX} \
        ${GROUP} ${MAX_CORES} ${EXEC_MEM}"

    _do_flink_func
}

do_terasort_flk()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

	SOURCE_PATH=$1
	TARGET_PATH=$2
	REDS=$((${3} * ${HOSTS_NUM}))
	JOB_NAME="Flink Terasort"

    cmd="${FLINK_HOME}/bin/flink \
        run \
        -c microbench.terasort.TeraSort \
        ${FLINK_BENCH_JAR} \
        ${HDFS_MASTER} \
        ${SOURCE_PATH} \
        ${TARGET_PATH} \
        ${REDS}"

    _do_flink_func
}

do_pagerank_flk()
{
    echo
}

do_kmeans_flk()
{

	SOURCE_PATH=$1
	K_CENTERS=$2
	MAX_ITERATION=$3

	JOB_NAME="Flink Kmeans"

    cmd=""

    _do_flink_func
}

