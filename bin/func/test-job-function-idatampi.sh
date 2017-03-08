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
_do_dmi_func()
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

do_pagerank_dmi()
{

    SOURCE_PATH=$1
	OUTPUT=$2
    ITER_COUNT=$3
	PROC_NUM=$4

	JOB_NAME="MPID PageRank"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
		-mode ITER -O $PROC_NUM -A $PROC_NUM -jar ${DATAMPI_BENCH_JAR} \
        mlbench.pagerank.PageRank \
        $SOURCE_PATH $ITER_COUNT $OUTPUT"

	_do_dm_func
}

do_kmeans_dmi()
{

    SOURCE_PATH=$1
	CENTER_SOURCE=$2
    ITER_COUNT=$3
	PROC_NUM=$4
    CENTER_NUMBER=$5
    VEC_DIMENSION=$6

	JOB_NAME="MPID K-means"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
		-mode ITER -O $PROC_NUM -A $PROC_NUM -jar ${DATAMPI_BENCH_JAR} \
        mlbench.kmeans.Kmeans \
        $SOURCE_PATH $CENTER_SOURCE $CENTER_NUMBER $VEC_DIMENSION $ITER_COUNT"

	_do_dm_func
}
