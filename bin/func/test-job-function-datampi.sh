#!/usr/bin/env bash

file="$_TESTDIR/func/test-job-function-basic.sh"
if [ -f "$file" ]; then
    . "$file"
else
    echo "No $file found."
    exit 1
fi

[ "$IMPORT_BASIC" != "true" ] && echo "Please import the basic job function script 'test-job-function-basic.sh'" && exit 1

_test_mpi_start()
{
    MPI_NOACK_TAG='no msg recvd from mpd when expecting ack of request'

    startLine=$1
    startLine=$((startLine+1))
    isNoAck=`sed -n "${startLine},$"p ${LOG_NAME} | grep "${MPI_NOACK_TAG}"`
    if [ "$isNoAck" != "" ]
    then
        echo 1
    else
        echo 0
    fi
}

_check_mpi_start()
{
    startLine=$1
    PId=$2
    chk_cnt=3
    while [ "$chk_cnt" != "0" ]
    do
        if [ "`_test_mpi_start $startLine`" = "1" ]
        then
            kill  $PId
            for mpid in `jps | grep MPI_D | awk '{print $1}'`;do kill $mpid;done
            kill $PId
            echo 1
            break
        else
            sleep 2
        fi
        chk_cnt=$((chk_cnt - 1))
    done
    echo 0
}

##  DataMPI jobs
_do_dm_func()
{
    # need set TARGET_PATH, SOURCE_PATH, JOB_NAME, HADOOP_HOME, LOG_NAME, REPORT_NAME
#   del_data ${TARGET_PATH}
#    isSrcExist=`${HADOOP_HOME}/bin/hadoop fs -ls ${SOURCE_PATH}`
#    if [ "$isSrcExist" = "" ]
#    then
#        echo "[FAIL] DataMPI $JOB_NAME `basename $SOURCE_PATH` MAPS $MAPS REDUCES $REDS. Doesn't have $SOURCE_PATH" | tee -a $REPORT_NAME
#        return 1
#    fi

    i=1
    for((;i!=0;i--)){
        [ ! -f "${LOG_NAME}" ] && touch ${LOG_NAME}
        startLine=`wc -l "$LOG_NAME" | awk '{print $1}'`
        echo ${cmd} | tee -a ${LOG_NAME}
		t1=`date +%s`
		case ${cmd} in
			"f_pagerank")
			run_pagerank 2>&1 | tee -a ${LOG_NAME} &
			;;
			*)
        	bash -c "${cmd}" 2>&1 | tee -a ${LOG_NAME} &
			;;
		esac
        PId=$!
        if [ "`_check_mpi_start $startLine $PId`" = "0" ]
        then
            break
        fi
    }

    if [ "$i" != "0" ]
    then
        wait $PId

        if [ "$?" != "0" -o "`check_log ${startLine}`" != "0" ]
        then
             echo "[FAIL] DataMPI $JOB_NAME `basename $SOURCE_PATH` MAPS $MAPS REDUCES $REDS. " | tee -a $REPORT_NAME
            TEST_STAT=1
        else
		    t2=`date +%s`
            costTime=`get_running_time 'mpid' ${startLine}`
            echo "[OK] DataMPI $JOB_NAME `basename $SOURCE_PATH` hosts $HOSTS_NUM app spend ${costTime}, total cost $((t2-t1)) sec" | tee -a $REPORT_NAME
        fi
    else
        echo "[FAIL] DataMPI $JOB_NAME `basename $SOURCE_PATH` MAPS $MAPS REDUCES $REDS. mpi start with no ack recv " | tee -a $REPORT_NAME
    fi

    startLine=0
}

do_sleep_dm()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`
    SECOND=${1}
    MAPS=$((${2} * ${HOSTS_NUM}))
    REDS=$((${3} * ${HOSTS_NUM}))

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_BENCH_JAR} test.Sleep ${SECOND}"

    _do_dm_func
}

do_terasort_dm()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    MAPS=$((${3} * ${HOSTS_NUM}))
    REDS=$((${4} * ${HOSTS_NUM}))
    JOB_NAME="TeraSort"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_BENCH_JAR} microbench.TeraSortOnHDFSDataLocal \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH}"

    _do_dm_func
}

do_text_wc_dm()
{
    ##  use text data
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    MAPS=$((${3} * ${HOSTS_NUM}))
    JOB_NAME="textwc"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${MAPS} \
        -jar ${DATAMPI_BENCH_JAR} microbench.WordCountOnHDFSDataLocal \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH}"

    _do_dm_func
}

do_text_sort_dm()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    MAPS=$((${3} * ${HOSTS_NUM}))
    REDS=$((${4} * ${HOSTS_NUM}))
    JOB_NAME="textstlocal"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_BENCH_JAR} microbench.SortOnHDFSDataLocal \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH}"

    _do_dm_func
}

do_bytes_sort_dm()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    MAPS=$((${3} * ${HOSTS_NUM}))
    REDS=$((${4} * ${HOSTS_NUM}))
    JOB_NAME="seqst"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_BENCH_JAR} mpicrobench.BytesSortOnHDFS \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH}"

    _do_dm_func
}

do_text_grep_dm()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    MAPS=$((${3} * ${HOSTS_NUM}))
    REDS=$MAPS
    JOB_NAME="textgrep"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_BENCH_JAR} mpicrobench.GrepOnHDFSDataLocal \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH} princess"

    _do_dm_func

}

do_kmeans_dm()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    POINT_PATH=${1}
    TARGET_PATH=${2}
    CENTERS_PATH=${TARGET_PATH}/center0
    MAPS=$((${3} * ${HOSTS_NUM}))
    REDS=$((${4} * ${HOSTS_NUM}))
    KCENTERS=${5}
    ITER=${6:-1}
    JOB_NAME="kmeans"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_BENCH_JAR} \
        mlbench.kmeans.KmeansInit \
        ${HDFS_CONF_CORE} ${POINT_PATH} ${CENTERS_PATH} $KCENTERS"

    isSrcExist=`${HADOOP_HOME}/bin/hadoop fs -ls ${SOURCE_PATH}`
    if [ "$isSrcExist" = "" ]
    then
        echo "[FAIL] DataMPI $JOB_NAME `basename $SOURCE_PATH` MAPS $MAPS REDUCES $REDS. Doesn't have $SOURCE_PATH" | tee -a $REPORT_NAME
        return 1
    fi

    [ ! -f "${LOG_NAME}" ] && touch ${LOG_NAME}
    echo ${cmd} | tee -a ${LOG_NAME}
	t1=`date +%s`
    ${cmd} 2>&1 | tee -a ${LOG_NAME}
    t2=`date +%s`
    echo "Prepare cost $((t2-t1)) sec" | tee -a ${LOG_NAME}

    for i in `seq $ITER`; do

        PREV_CENTERS_PATH=$CENTERS_PATH
        CENTERS_PATH="$TARGET_PATH/out${i}"

        cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
            -mode COM -O ${MAPS} -A ${REDS} \
            -jar ${DATAMPI_BENCH_JAR} \
            mlbench.kmeans.KmeansIter \
            ${HDFS_CONF_CORE} ${POINT_PATH} ${PREV_CENTERS_PATH} ${CENTERS_PATH} $KCENTERS"

        echo ${cmd} | tee -a ${LOG_NAME}
        ${cmd} 2>&1 | tee -a ${LOG_NAME}
        t3=`date +%s`
        echo "Iter $i cost $((t3-t2)) sec" | tee -a ${LOG_NAME}
        t2=$t3

    done

    echo "[OK] DataMPI $JOB_NAME `basename $POINT_PATH` hosts $HOSTS_NUM app iteration $MAX_ITER_NUM, total cost $((t2-t1)) sec" | tee -a $REPORT_NAME
}

do_pagerank_dm()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    INIT_VEC_PATH=${2}
    TARGET_PATH="${3}/pr_target_vec"
	MAPS=$((${4} * ${HOSTS_NUM}))
    REDS=$((${5} * ${HOSTS_NUM}))
    VEC_NUM=${6}
    MAX_ITER_NUM=${7}

    VEC_PATH="${3}/pr_temp_vec"
    MODEL_PATH="${3}/pr_temp_model"

    del_data ${VEC_PATH%\/*}
	${HADOOP_HOME}/bin/hadoop fs -mkdir ${VEC_PATH%\/*}
    copy_data ${INIT_VEC_PATH} ${VEC_PATH}

	ts_pr=`date +%s`
    for i in `seq 1 $MAX_ITER_NUM`;
    do
        echo "ITER: loop $i"
        echo "First Stage ......."

        del_data ${MODEL_PATH}

        cmd="${MPI_D_HOME}/bin/mpidrun \
            -f ${MPI_D_SLAVES} -mode COM \
            -O ${MAPS} -A ${REDS}  \
            -jar ${DATAMPI_BENCH_JAR} mlbench.pagerank.PagerankNaive \
            ${HDFS_CONF_CORE} ${SOURCE_PATH} ${VEC_PATH} ${MODEL_PATH}"

        _do_dm_func

        echo "Second Stage ......"

        del_data ${VEC_PATH}

        cmd="${MPI_D_HOME}/bin/mpidrun \
            -f ${MPI_D_SLAVES} -mode COM \
            -O ${MAPS} -A ${REDS}  \
            -jar ${DATAMPI_BENCH_JAR} mlbench.pagerank.PagerankMerge \
            ${HDFS_CONF_CORE} ${MODEL_PATH} ${VEC_PATH} \
            ${VEC_NUM} 0.85"

        _do_dm_func

        if [ $i -eq $MAX_ITER_NUM ]; then
            echo "PageRank has reached the maxium iteration limits. Now we have to finish..."
            move_data ${VEC_PATH} ${TARGET_PATH}
            break
        fi
    done

    ed_pr=`date +%s`
    echo "[OK] DataMPI $JOB_NAME `basename $SOURCE_PATH` hosts $HOSTS_NUM app iteration $MAX_ITER_NUM, total cost $((ed_pr-ts_pr)) sec" | tee -a $REPORT_NAME

}

new_pagerank(){

    SOURCE_PATH=$1
	OUTPUT=$2
    ITER_COUNT=$3
	PROC_NUM=$4

	JOB_NAME="MPID PageRank"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
		-mode ITER -O $PROC_NUM -A $PROC_NUM -jar ${DATAMPI_BENCH_JAR} \
        mpid.iteration.examples.pagerank.PageRank \
        $SOURCE_PATH $ITER_COUNT $OUTPUT"

	_do_dm_func
}

new_kmeans(){

    SOURCE_PATH=$1
	CENTER_SOURCE=$2
    ITER_COUNT=$3
	PROC_NUM=$4
    CENTER_NUMBER=$5
    VEC_DIMENSION=$6

	JOB_NAME="MPID K-means"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
		-mode ITER -O $PROC_NUM -A $PROC_NUM -jar ${DATAMPI_BENCH_JAR} \
        mpid.iteration.examples.Kmeans \
        $SOURCE_PATH $CENTER_SOURCE $CENTER_NUMBER $VEC_DIMENSION $ITER_COUNT"

	_do_dm_func
}
