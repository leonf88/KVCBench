#! /usr/bin/env bash
# Workload
# $1 specifies the job type. 
# Author : Lu CHAO(me@chao.lu)
# Date : 2014/04/22  
# Modified by : Fan Liang

source basic.sh
ldfunc

case $1 in
  "test_mpid")
      S_DIR=tiny
      M_TAR=tiny-mpid
      do_text_wc_mpid_local "${S_DIR}" "${M_TAR}-wc" "1"
      # do_text_sort_mpid_local "${S_DIR}" "${M_TAR}-st" "1" "1"
      # do_text_grep_mpid "${S_DIR}" "${M_TAR}-gp" "1"
  ;;
  "test_had")
      S_DIR=tiny
      H_TAR=tiny-had
      do_text_sort_had "${S_DIR}" "${H_TAR}-st" "1"
      # do_text_wc_had "${S_DIR}" "${H_TAR}-wc"
      # do_text_grep_had "${S_DIR}" "${H_TAR}-gp"
  ;;
# Hadoop Jobs
  "30M_PR_HAD_1I")
      S_DIR=/data/pagerank/30M
      V_DIR=/data/pagerank/hadoop/30M/vec
      H_TAR=/output/pagerank/hadoop
      let "N=2**25"

      do_pagerank_had $S_DIR $H_TAR $N 4 1
  ;;
  "1M_PR_HAD_1I")
      S_DIR=/data/pagerank/1M
      V_DIR=/data/pagerank/hadoop/1M/vec
      H_TAR=/output/pagerank/hadoop
      let "N=2**20"

      do_pagerank_had $S_DIR $H_TAR $N 4 1
  ;;
  "web-Google_HAD")
      S_DIR=/data/pagerank/web-Google
      V_DIR=/data/pagerank/hadoop/web-Google/vec
      H_TAR=/output/pagerank/hadoop
      N=875713

      do_pagerank_had $S_DIR $H_TAR $N 4 10
  ;;
  "LiveJournal1_HAD")
      S_DIR=/data/pagerank/soc-LiveJournal1
      V_DIR=/data/pagerank/hadoop/soc-LiveJournal1/vec
      H_TAR=/output/pagerank/hadoop
      N=4847571

      do_pagerank_had $S_DIR $H_TAR $N 4 10
  ;;
  "com-friendster")
      S_DIR=/data/pagerank/com-friendster
      V_DIR=/data/pagerank/hadoop/com-friendster/vec
      H_TAR=/output/pagerank/hadoop
      N=65608366

      do_pagerank_had $S_DIR $H_TAR $N 4 10
  ;;
  "1M_PR_HAD")
      S_DIR=/data/pagerank/1M
      V_DIR=/data/pagerank/hadoop/1M/vec
      H_TAR=/output/pagerank/hadoop
      let "N=2**20"

      do_pagerank_had $S_DIR $H_TAR $N 4 10
  ;;
  "10M_PR_HAD")
      S_DIR=/data/pagerank/10M
      V_DIR=/data/pagerank/hadoop/10M/vec
      H_TAR=/output/pagerank/hadoop
      let "N=2**23"

      do_pagerank_had $S_DIR $H_TAR $N 4 10
  ;;
  "30M_PR_HAD")
      S_DIR=/data/pagerank/30M
      V_DIR=/data/pagerank/hadoop/30M/vec
      H_TAR=/output/pagerank/hadoop/30M
      let "N=2**25"

      do_pagerank_had $S_DIR $H_TAR $N 4 10
  ;;
  "KDD_KM_HAD")
      S_DIR=/kmeans/data/data_kddcup04
      H_TAR=/kmeans/hadoop
      CENTERS_PATH=/kmeans/data/centers/kdd-biotrain/part-randomSeed

      do_kmeans_had $S_DIR $H_TAR $CENTERS_PATH 4 25 10 0.5
  ;;
  "1M_KM_HAD")
      S_DIR=/kmeans/data/1M
      H_TAR=/kmeans/hadoop
      CENTERS_PATH=/kmeans/data/centers/self-generate/part-randomSeed

      do_kmeans_had $S_DIR $H_TAR $CENTERS_PATH 4 25 10 0.5
  ;;
  "10M_KM_HAD")
      S_DIR=/kmeans/data/10M
      H_TAR=/kmeans/hadoop
      CENTERS_PATH=/kmeans/data/centers/self-generate/part-randomSeed

      do_kmeans_had $S_DIR $H_TAR $CENTERS_PATH 4 25 10 0.5
  ;;
  "30M_KM_HAD")
      S_DIR=/kmeans/data/30M
      H_TAR=/kmeans/hadoop
      CENTERS_PATH=/kmeans/data/centers/self-generate/part-randomSeed

      do_kmeans_had $S_DIR $H_TAR $CENTERS_PATH 4 25 10 0.5
  ;;

# DataMPI Jobs
  "Friendster_DM")

  S_DIR=/data/pagerank/com-friendster
  M_TAR=/pagerank/mpid

  new_pagerank "${S_DIR}" "${M_TAR}" "2" "14" 
  ;;

  "LiveJournal1_DM")

  S_DIR=/data/pagerank/soc-LiveJournal1
  M_TAR=/pagerank/mpid

  new_pagerank "${S_DIR}" "${M_TAR}" "10" "14"
  ;;

  "web-Google_DM")

  S_DIR=/data/pagerank/web-Google
  M_TAR=/pagerank/mpid

  new_pagerank "${S_DIR}" "${M_TAR}" "10" "14" 
  ;;

  "30M_PR_DM")

  S_DIR=/data/pagerank/30M
  M_TAR=/pagerank/mpid

  new_pagerank "${S_DIR}" "${M_TAR}" "10" "14" 
  ;;

  "1M_PR_DM")

  S_DIR=/data/pagerank/1M
  M_TAR=/pagerank/mpid

  new_pagerank "${S_DIR}" "${M_TAR}" "10" "14" 
  ;;

  "10M_PR_DM")

  S_DIR=/data/pagerank/10M
  M_TAR=/pagerank/mpid

  new_pagerank "${S_DIR}" "${M_TAR}" "10" "14" 
  ;;
  "KM_10M_DM")

  S_DIR=/kmeans/data/10M
  CENTER_SOURCE=/kmeans/data/centers/centers.100d.25p
  CENTER_NUMBER=25
  VEC_DIMENSION=100
  ITER_COUNT=10

  new_kmeans "${S_DIR}" "${CENTER_SOURCE}" "$ITER_COUNT" "14" "$CENTER_NUMBER" "$VEC_DIMENSION"
  ;;
  "KM_30M_DM")

  S_DIR=/kmeans/data/30M
  CENTER_SOURCE=/kmeans/data/centers/centers.100d.25p
  CENTER_NUMBER=25
  VEC_DIMENSION=100
  ITER_COUNT=10

  new_kmeans "${S_DIR}" "${CENTER_SOURCE}" "$ITER_COUNT" "14" "$CENTER_NUMBER" "$VEC_DIMENSION"
  ;;
  "KM_1M_DM")

  S_DIR=/kmeans/data/1M
  CENTER_SOURCE=/kmeans/data/centers/centers.100d.25p
  CENTER_NUMBER=25
  VEC_DIMENSION=100
  ITER_COUNT=10

  new_kmeans "${S_DIR}" "${CENTER_SOURCE}" "$ITER_COUNT" "14" "$CENTER_NUMBER" "$VEC_DIMENSION"
  ;;

  "KM_KDD_DM")

  S_DIR=/kmeans/data/data_kddcup04
  CENTER_SOURCE=/kmeans/data/centers/centers.kdd
  CENTER_NUMBER=25
  VEC_DIMENSION=74
  ITER_COUNT=10

  new_kmeans "${S_DIR}" "${CENTER_SOURCE}" "$ITER_COUNT" "14" "$CENTER_NUMBER" "$VEC_DIMENSION"
  ;;

# Spark Jobs
  "30M_PR_SPK_5I")
      S_DIR=/data/pagerank/30M
      P_TAR=/output/pagerank/spark

      do_pagerank $S_DIR $P_TAR "5"
  ;;
  "30M_PR_SPK_1I")
      S_DIR=/data/pagerank/30M
      P_TAR=/output/pagerank/spark

      do_pagerank $S_DIR $P_TAR "1"
  ;;
  "1M_PR_SPK_1I")
      S_DIR=/data/pagerank/1M
      P_TAR=/output/pagerank/spark

      do_pagerank $S_DIR $P_TAR "1"
  ;;
  "LiveJournal1_SPK")
      S_DIR=/data/pagerank/soc-LiveJournal1
      P_TAR=/output/pagerank/spark

      do_pagerank $S_DIR $P_TAR "10"
  ;;
  "web-Google_SPK")
      S_DIR=/data/pagerank/web-Google
      P_TAR=/output/pagerank/spark

      do_pagerank $S_DIR $P_TAR "10"
  ;;
  "com-friendster_SPK")
      S_DIR=/data/pagerank/com-friendster
      P_TAR=/output/pagerank/spark

      do_pagerank $S_DIR $P_TAR "10"
  ;;
  "1M_PR_SPK")
      S_DIR=/data/pagerank/1M
      P_TAR=/output/pagerank/spark

      do_pagerank $S_DIR $P_TAR "10"
  ;;
  "10M_PR_SPK")
      S_DIR=/data/pagerank/10M
      P_TAR=/output/pagerank/spark

      do_pagerank $S_DIR $P_TAR "10"
  ;;
  "30M_PR_SPK")
      S_DIR=/data/pagerank/30M
      P_TAR=/output/pagerank/spark

      do_pagerank $S_DIR $P_TAR "10"
  ;;
  
  "1M_KM_SPK")

      SOURCE_PATH=/kmeans/data/1M
      CENTERS_PATH=/kmeans/data/centers/centers.100d.25p
      K_CENTERS=25
      ITER_NUM=10

      do_kmeans2 $SOURCE_PATH $CENTERS_PATH $K_CENTERS $ITER_NUM
  ;;
  
  "10M_KM_SPK")
      SOURCE_PATH=/kmeans/data/10M
      CENTERS_PATH=/kmeans/data/centers/centers.100d.25p
      K_CENTERS=25
      ITER_NUM=10

      do_kmeans2 $SOURCE_PATH $CENTERS_PATH $K_CENTERS $ITER_NUM
  ;;
  
  "30M_KM_SPK")
      SOURCE_PATH=/kmeans/data/30M
      CENTERS_PATH=/kmeans/data/centers/centers.100d.25p
      K_CENTERS=25
      ITER_NUM=10

      do_kmeans2 $SOURCE_PATH $CENTERS_PATH $K_CENTERS $ITER_NUM
  ;;

  "KDD_KM_SPK")
      SOURCE_PATH=/kmeans/data/data_kddcup04
      CENTERS_PATH=/kmeans/data/centers/centers.kdd
      K_CENTERS=25
      ITER_NUM=10

      do_kmeans2 $SOURCE_PATH $CENTERS_PATH $K_CENTERS $ITER_NUM
  ;;
  "30M_PR_GRX")
      SOURCE_PATH=/data/pagerank/30M
      OUTPUT_PATH=/pagerank/spark
      ITER_NUM=10
      E_PART=28

      do_pagerank_graphx ${SOURCE_PATH} ${OUTPUT_PATH} ${ITER_NUM} ${E_PART}
  ;;

# self-test
  *)
    echo "Job undefined!"
  ;;
esac
