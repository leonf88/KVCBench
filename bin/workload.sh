#! /usr/bin/env bash
# Workload
# $1 specifies the job type. 
# Author : Lu CHAO(me@chao.lu)
# Date : 2014/04/22  
# Modified by : Fan Liang
# Naming format: <Dataset>_<Bench_Type>_<Platform>

source basic.sh
ldfunc

case $1 in
  "64G_TERA_DM")
      S_DIR=/data/terasort/64G-tera
      OUTPUT_HDFS=/output/dm/tera

      do_terasort_dm $S_DIR $OUTPUT_HDFS 1 1
  ;;
  "64G_TERA_HAD")
      S_DIR=/data/terasort/64G-tera
      OUTPUT_HDFS=/output/hadoop/tera

      do_terasort_had $S_DIR $OUTPUT_HDFS 1
  ;;
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

  # Hadoop PageRank Jobs
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
  "web-Google_PR_HAD")
      S_DIR=/data/pagerank/web-Google
      V_DIR=/data/pagerank/hadoop/web-Google/vec
      H_TAR=/output/pagerank/hadoop
      N=875713

      do_pagerank_had $S_DIR $H_TAR $N 4 10
  ;;
  "LiveJournal1_PR_HAD")
      S_DIR=/data/pagerank/soc-LiveJournal1
      V_DIR=/data/pagerank/hadoop/soc-LiveJournal1/vec
      H_TAR=/output/pagerank/hadoop
      N=4847571

      do_pagerank_had $S_DIR $H_TAR $N 4 10
  ;;
  "com-friendster_PR_HAD")
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

  # Hadoop KMeans Jobs
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
  "KDD04_KM_HAD")
      S_DIR=/data/kmeans/data_kddcup04/data
      H_TAR=/data/kmeans/data_kddcup04/cluster
      OUTPUT_HDFS=/output/kmeans/hadoop

      do_kmeans_had $S_DIR $H_TAR $OUTPUT_HDFS 10 0.5
  ;;
  "1M_KM_HAD_NEW")
      S_DIR=/data/kmeans/1M/data
      H_TAR=/data/kmeans/1M/cluster
      OUTPUT_HDFS=/output/kmeans/hadoop/1M

      do_kmeans_had $S_DIR $H_TAR $OUTPUT_HDFS 10 0.5
  ;;
  "10M_KM_HAD_NEW")
      S_DIR=/data/kmeans/10M/data
      H_TAR=/data/kmeans/10M/cluster
      OUTPUT_HDFS=/output/kmeans/hadoop/10M

      do_kmeans_had $S_DIR $H_TAR $OUTPUT_HDFS 10 0.5
  ;;
  "30M_KM_HAD_NEW")
      S_DIR=/data/kmeans/30M/data
      H_TAR=/data/kmeans/30M/cluster
      OUTPUT_HDFS=/output/kmeans/hadoop/30M

      do_kmeans_had $S_DIR $H_TAR $OUTPUT_HDFS 10 0.5
  ;;
  "60M_KM_HAD_NEW")
      S_DIR=/data/kmeans/60M/data
      H_TAR=/data/kmeans/60M/cluster
      OUTPUT_HDFS=/output/kmeans/hadoop/60M

      do_kmeans_had $S_DIR $H_TAR $OUTPUT_HDFS 10 0.5
  ;;

  # Hadoop Terasort Jobs
  "2G_TERA_HAD")
      S_DIR=/data/terasort/2G-tera
      OUTPUT_HDFS=/output/tera/2G

      do_terasort_had $S_DIR $OUTPUT_HDFS 1
  ;;
  "10G_TERA_HAD")
      S_DIR=/data/terasort/10G-tera
      OUTPUT_HDFS=/output/hadoop/tera

      do_terasort_had $S_DIR $OUTPUT_HDFS 4
  ;;
  "20G_TERA_HAD")
      S_DIR=/data/terasort/20G-tera
      OUTPUT_HDFS=/output/hadoop/tera

      do_terasort_had $S_DIR $OUTPUT_HDFS 4
  ;;
  "40G_TERA_HAD")
      S_DIR=/data/terasort/40G-tera
      OUTPUT_HDFS=/output/hadoop/tera

      do_terasort_had $S_DIR $OUTPUT_HDFS 4
  ;;
  "80G_TERA_HAD")
      S_DIR=/data/terasort/80G-tera
      OUTPUT_HDFS=/output/hadoop/tera

      do_terasort_had $S_DIR $OUTPUT_HDFS 4
  ;;
  "160G_TERA_HAD")
      S_DIR=/data/terasort/160G-tera
      OUTPUT_HDFS=/output/hadoop/tera

      do_terasort_had $S_DIR $OUTPUT_HDFS 4
  ;;


  # Hadoop Sort Jobs
  "2G_ST_HAD")
      S_DIR=/data/text/2G-text
      OUTPUT_HDFS=/output/st/hadoop/2G

      do_text_sort_had $S_DIR $OUTPUT_HDFS 1
  ;;
  "10G_ST_HAD")
      S_DIR=/data/text/10G-text
      OUTPUT_HDFS=/output/hadoop/st

      do_text_sort_had $S_DIR $OUTPUT_HDFS 4
  ;;
  "20G_ST_HAD")
      S_DIR=/data/text/20G-text
      OUTPUT_HDFS=/output/hadoop/st

      do_text_sort_had $S_DIR $OUTPUT_HDFS 4
  ;;
  "40G_ST_HAD")
      S_DIR=/data/text/40G-text
      OUTPUT_HDFS=/output/hadoop/st

      do_text_sort_had $S_DIR $OUTPUT_HDFS 4
  ;;
  "80G_ST_HAD")
      S_DIR=/data/text/80G-text
      OUTPUT_HDFS=/output/hadoop/st

      do_text_sort_had $S_DIR $OUTPUT_HDFS 4
  ;;
  "160G_ST_HAD")
      S_DIR=/data/text/160G-text
      OUTPUT_HDFS=/output/hadoop/st

      do_text_sort_had $S_DIR $OUTPUT_HDFS 4
  ;;

  # Hadoop WordCount Jobs
  "2G_WC_HAD")
      S_DIR=/data/text/2G-text
      OUTPUT_HDFS=/output/wc/2G

      do_text_wc_had $S_DIR $OUTPUT_HDFS 1
  ;;
  "10G_WC_HAD")
      S_DIR=/data/text/10G-text
      OUTPUT_HDFS=/output/hadoop/wc

      do_text_wc_had $S_DIR $OUTPUT_HDFS 4
  ;;
  "20G_WC_HAD")
      S_DIR=/data/text/20G-text
      OUTPUT_HDFS=/output/hadoop/wc

      do_text_wc_had $S_DIR $OUTPUT_HDFS 4
  ;;
  "40G_WC_HAD")
      S_DIR=/data/text/40G-text
      OUTPUT_HDFS=/output/hadoop/wc

      do_text_wc_had $S_DIR $OUTPUT_HDFS 4
  ;;
  "80G_WC_HAD")
      S_DIR=/data/text/80G-text
      OUTPUT_HDFS=/output/hadoop/wc

      do_text_wc_had $S_DIR $OUTPUT_HDFS 4
  ;;
  "160G_WC_HAD")
      S_DIR=/data/text/160G-text
      OUTPUT_HDFS=/output/hadoop/wc

      do_text_wc_had $S_DIR $OUTPUT_HDFS 4
  ;;

  # DataMPI Test
  "0_SLEEP_DM")
      do_sleep_dm 5 8 8
  ;;
  # DataMPI Terasort Jobs
  "2G_TERA_DM")
      S_DIR=/data/terasort/2G-tera
      OUTPUT_HDFS=/output/tera/2G

      do_terasort_dm $S_DIR $OUTPUT_HDFS 1 1
  ;;
  "10G_TERA_DM")
      S_DIR=/data/terasort/10G-tera
      OUTPUT_HDFS=/output/dm/tera

      do_terasort_dm $S_DIR $OUTPUT_HDFS 4 4
  ;;
  "20G_TERA_DM")
      S_DIR=/data/terasort/20G-tera
      OUTPUT_HDFS=/output/dm/tera

      do_terasort_dm $S_DIR $OUTPUT_HDFS 4 4
  ;;
  "40G_TERA_DM")
      S_DIR=/data/terasort/40G-tera
      OUTPUT_HDFS=/output/dm/tera

      do_terasort_dm $S_DIR $OUTPUT_HDFS 4 4
  ;;
  "80G_TERA_DM")
      S_DIR=/data/terasort/80G-tera
      OUTPUT_HDFS=/output/dm/tera

      do_terasort_dm $S_DIR $OUTPUT_HDFS 4 4
  ;;
  "160G_TERA_DM")
      S_DIR=/data/terasort/160G-tera
      OUTPUT_HDFS=/output/dm/tera

      do_terasort_dm $S_DIR $OUTPUT_HDFS 4 4
  ;;

  # DataMPI Sort Jobs
  "2G_ST_DM")
      S_DIR=/data/text/2G-text
      OUTPUT_HDFS=/output/st/dm/2G

      do_text_sort_dm $S_DIR $OUTPUT_HDFS 1 1
  ;;
  "10G_ST_DM")
      S_DIR=/data/text/10G-text
      OUTPUT_HDFS=/output/dm/st

      do_text_sort_dm $S_DIR $OUTPUT_HDFS 4 4
  ;;
  "20G_ST_DM")
      S_DIR=/data/text/20G-text
      OUTPUT_HDFS=/output/dm/st

      do_text_sort_dm $S_DIR $OUTPUT_HDFS 4 4
  ;;
  "40G_ST_DM")
      S_DIR=/data/text/40G-text
      OUTPUT_HDFS=/output/dm/st

      do_text_sort_dm $S_DIR $OUTPUT_HDFS 4 4
  ;;
  "80G_ST_DM")
      S_DIR=/data/text/80G-text
      OUTPUT_HDFS=/output/dm/st

      do_text_sort_dm $S_DIR $OUTPUT_HDFS 4 4
  ;;
  "160G_ST_DM")
      S_DIR=/data/text/160G-text
      OUTPUT_HDFS=/output/dm/st

      do_text_sort_dm $S_DIR $OUTPUT_HDFS 4 4
  ;;

  # DataMPI WordCount Jobs
  "2G_WC_DM")
      S_DIR=/data/text/2G-text
      OUTPUT_HDFS=/output/wc/2G

      do_text_wc_dm $S_DIR $OUTPUT_HDFS 1 1
  ;;
  "10G_WC_DM")
      S_DIR=/data/text/10G-text
      OUTPUT_HDFS=/output/dm/wc

      do_text_wc_dm $S_DIR $OUTPUT_HDFS 5 5
  ;;
  "20G_WC_DM")
      S_DIR=/data/text/20G-text
      OUTPUT_HDFS=/output/dm/wc

      do_text_wc_dm $S_DIR $OUTPUT_HDFS 5 5
  ;;
  "40G_WC_DM")
      S_DIR=/data/text/40G-text
      OUTPUT_HDFS=/output/dm/wc

      do_text_wc_dm $S_DIR $OUTPUT_HDFS 5 5
  ;;
  "80G_WC_DM")
      S_DIR=/data/text/80G-text
      OUTPUT_HDFS=/output/dm/wc

      do_text_wc_dm $S_DIR $OUTPUT_HDFS 5 5
  ;;
  "160G_WC_DM")
      S_DIR=/data/text/160G-text
      OUTPUT_HDFS=/output/dm/wc

      do_text_wc_dm $S_DIR $OUTPUT_HDFS 5 5
  ;;

  # DataMPI PageRank Jobs
  "1M_PR_DM_TEST")
      S_DIR=/data/pagerank/1M
      V_DIR=/data/pagerank/1M_dm_init_vector
      M_TAR=/pagerank/mpid
      let "N=2**20"

      do_pagerank_dm "${S_DIR}" "${V_DIR}" "${M_TAR}" 1 1 ${N} 1
  ;;
  "Friendster_PR_DM")
      S_DIR=/data/pagerank/com-friendster
      V_DIR=/data/pagerank/com-friendster_dm_init_vector
      M_TAR=/pagerank/mpid
      N=65608366

      do_pagerank_dm "${S_DIR}" "${V_DIR}" "${M_TAR}" 4 4 ${N} 10
  ;;
  "LiveJournal1_PR_DM")
      S_DIR=/data/pagerank/soc-LiveJournal1
      V_DIR=/data/pagerank/soc-LiveJournal1_dm_init_vector
      M_TAR=/pagerank/mpid
      N=4847571

      do_pagerank_dm "${S_DIR}" "${V_DIR}" "${M_TAR}" 4 4 ${N} 10
  ;;
  "web-Google_PR_DM")
      S_DIR=/data/pagerank/web-Google
      V_DIR=/data/pagerank/web-Google_dm_init_vector
      M_TAR=/pagerank/mpid
      N=875713

      do_pagerank_dm "${S_DIR}" "${V_DIR}" "${M_TAR}" 1 1 ${N} 10
  ;;
  "30M_PR_DM")
      S_DIR=/data/pagerank/30M
      V_DIR=/data/pagerank/30M_dm_init_vector
      M_TAR=/pagerank/mpid
      let "N=2**25"

      do_pagerank_dm "${S_DIR}" "${V_DIR}" "${M_TAR}" 4 4 ${N} 10
  ;;
  "1M_PR_DM")
      S_DIR=/data/pagerank/1M
      V_DIR=/data/pagerank/1M_dm_init_vector
      M_TAR=/pagerank/mpid
      let "N=2**20"

      do_pagerank_dm "${S_DIR}" "${V_DIR}" "${M_TAR}" 1 1 ${N} 10
  ;;
  "10M_PR_DM")
      S_DIR=/data/pagerank/10M
      V_DIR=/data/pagerank/10M_dm_init_vector
      M_TAR=/pagerank/mpid
      let "N=2**23"

      do_pagerank_dm "${S_DIR}" "${V_DIR}" "${M_TAR}" 4 4 ${N} 10
  ;;

  # DataMPI KMeans Jobs
  "KDD04_KM_DM_TEST")
      S_DIR=/data/kmeans/data_kddcup04/data
      TARGET_PATH=/output/kmeans/kdd-dm
      CENTER_NUMBER=25
      ITER_COUNT=1

      do_kmeans_dm "${S_DIR}" "${TARGET_PATH}" 1 1 "${CENTER_NUMBER}" "${ITER_COUNT}"
  ;;

  "10M_KM_DM")
      S_DIR=/data/kmeans/10M/data
      TARGET_PATH=/output/kmeans/10M
      CENTER_NUMBER=25
      ITER_COUNT=10

      do_kmeans_dm "${S_DIR}" "${TARGET_PATH}" 4 4 "${CENTER_NUMBER}" "${ITER_COUNT}"
  ;;
  "30M_KM_DM")
      S_DIR=/data/kmeans/30M/data
      TARGET_PATH=/output/kmeans/30M
      CENTER_NUMBER=25
      ITER_COUNT=10

      do_kmeans_dm "${S_DIR}" "${TARGET_PATH}" 4 4 "${CENTER_NUMBER}" "${ITER_COUNT}"
  ;;
  "1M_KM_DM")
      S_DIR=/data/kmeans/1M/data
      TARGET_PATH=/output/kmeans/1M
      CENTER_NUMBER=25
      ITER_COUNT=10

      do_kmeans_dm "${S_DIR}" "${TARGET_PATH}" 4 4 "${CENTER_NUMBER}" "${ITER_COUNT}"
  ;;

  "KDD04_KM_DM")
      S_DIR=/data/kmeans/data_kddcup04/data
      TARGET_PATH=/output/kmeans/kdd
      CENTER_NUMBER=25
      ITER_COUNT=10

      do_kmeans_dm "${S_DIR}" "${TARGET_PATH}" 1 1 "${CENTER_NUMBER}" "${ITER_COUNT}"
  ;;

  # iDataMPI PageRank Jobs
  "Friendster_PR_DMI")
      S_DIR=/data/pagerank/com-friendster
      M_TAR=/pagerank/mpid

      do_pagerank_dmi "${S_DIR}" "${M_TAR}" "2" "14"
  ;;
  "LiveJournal1_PR_DMI")
      S_DIR=/data/pagerank/soc-LiveJournal1
      M_TAR=/pagerank/mpid

      do_pagerank_dmi "${S_DIR}" "${M_TAR}" "10" "14"
  ;;
  "web-Google_PR_DMI")
      S_DIR=/data/pagerank/web-Google
      M_TAR=/pagerank/mpid

      do_pagerank_dmi "${S_DIR}" "${M_TAR}" "10" "14"
  ;;
  "30M_PR_DMI")
      S_DIR=/data/pagerank/30M
      M_TAR=/pagerank/mpid

      do_pagerank_dmi "${S_DIR}" "${M_TAR}" "10" "14"
  ;;
  "1M_PR_DMI")
      S_DIR=/data/pagerank/1M
      M_TAR=/pagerank/mpid

      do_pagerank_dmi "${S_DIR}" "${M_TAR}" "10" "14"
  ;;
  "10M_PR_DMI")
      S_DIR=/data/pagerank/10M
      M_TAR=/pagerank/mpid

      do_pagerank_dmi "${S_DIR}" "${M_TAR}" "10" "14"
  ;;

  # iDataMPI KMeans Jobs
  "10M_KM_DMI")
      S_DIR=/kmeans/data/10M
      CENTER_SOURCE=/kmeans/data/centers/centers.100d.25p
      CENTER_NUMBER=25
      VEC_DIMENSION=100
      ITER_COUNT=10

      do_kmeans_dmi "${S_DIR}" "${CENTER_SOURCE}" "$ITER_COUNT" "14" "$CENTER_NUMBER" "$VEC_DIMENSION"
  ;;
  "30M_KM_DMI")
      S_DIR=/kmeans/data/30M
      CENTER_SOURCE=/kmeans/data/centers/centers.100d.25p
      CENTER_NUMBER=25
      VEC_DIMENSION=100
      ITER_COUNT=10

      do_kmeans_dmi "${S_DIR}" "${CENTER_SOURCE}" "$ITER_COUNT" "14" "$CENTER_NUMBER" "$VEC_DIMENSION"
  ;;
  "1M_KM_DMI")
      S_DIR=/kmeans/data/1M
      CENTER_SOURCE=/kmeans/data/centers/centers.100d.25p
      CENTER_NUMBER=25
      VEC_DIMENSION=100
      ITER_COUNT=10

      do_kmeans_dmi "${S_DIR}" "${CENTER_SOURCE}" "$ITER_COUNT" "14" "$CENTER_NUMBER" "$VEC_DIMENSION"
  ;;
  "KDD04_KM_DMI")
      S_DIR=/kmeans/data/data_kddcup04
      CENTER_SOURCE=/kmeans/data/centers/centers.kdd
      CENTER_NUMBER=25
      VEC_DIMENSION=74
      ITER_COUNT=10

      do_kmeans_dmi "${S_DIR}" "${CENTER_SOURCE}" "$ITER_COUNT" "14" "$CENTER_NUMBER" "$VEC_DIMENSION"
  ;;

# Spark PageRank Jobs
  "30M_PR_SPK_5I")
      S_DIR=/data/pagerank/30M
      P_TAR=/output/pagerank/spark

      do_pagerank_spk $S_DIR $P_TAR "5"
  ;;
  "30M_PR_SPK_1I")
      S_DIR=/data/pagerank/30M
      P_TAR=/output/pagerank/spark

      do_pagerank_spk $S_DIR $P_TAR "1"
  ;;
  "1M_PR_SPK_1I")
      S_DIR=/data/pagerank/1M
      P_TAR=/output/pagerank/spark

      do_pagerank_spk $S_DIR $P_TAR "1"
  ;;
  "LiveJournal1_PR_SPK")
      S_DIR=/data/pagerank/soc-LiveJournal1
      P_TAR=/output/pagerank/spark

      do_pagerank_spk $S_DIR $P_TAR "10"
  ;;
  "web-Google_PR_SPK")
      S_DIR=/data/pagerank/web-Google
      P_TAR=/output/pagerank/spark

      do_pagerank_spk $S_DIR $P_TAR "10"
  ;;
  "com-friendster_PR_SPK")
      S_DIR=/data/pagerank/com-friendster
      P_TAR=/output/pagerank/spark

      do_pagerank_spk $S_DIR $P_TAR "10"
  ;;
  "1M_PR_SPK")
      S_DIR=/data/pagerank/1M
      P_TAR=/output/pagerank/spark

      do_pagerank_spk $S_DIR $P_TAR "10"
  ;;
  "10M_PR_SPK")
      S_DIR=/data/pagerank/10M
      P_TAR=/output/pagerank/spark

      do_pagerank_spk $S_DIR $P_TAR "10"
  ;;
  "30M_PR_SPK")
      S_DIR=/data/pagerank/30M
      P_TAR=/output/pagerank/spark

      do_pagerank_spk $S_DIR $P_TAR "10"
  ;;
  
   # Spark KMeans Jobs
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
  "KDD04_KM_SPK")
      SOURCE_PATH=/data/kmeans/data_kddcup04/data
      K_CENTERS=25
      ITER_NUM=10

      do_kmeans_spk $SOURCE_PATH $K_CENTERS $ITER_NUM
  ;;
  "1M_KM_SPK_NEW")
      SOURCE_PATH=/data/kmeans/1M/data
      K_CENTERS=25
      ITER_NUM=10

      do_kmeans_spk $SOURCE_PATH $K_CENTERS $ITER_NUM
  ;;
  "10M_KM_SPK_NEW")
      SOURCE_PATH=/data/kmeans/10M/data
      K_CENTERS=25
      ITER_NUM=10

      do_kmeans_spk $SOURCE_PATH $K_CENTERS $ITER_NUM
  ;;
  "30M_KM_SPK_NEW")

      SOURCE_PATH=/data/kmeans/30M/data
      K_CENTERS=25
      ITER_NUM=10

      do_kmeans_spk $SOURCE_PATH $K_CENTERS $ITER_NUM
  ;;
  "60M_KM_SPK_NEW")
      SOURCE_PATH=/data/kmeans/60M/data
      K_CENTERS=25
      ITER_NUM=10

      do_kmeans_spk $SOURCE_PATH $K_CENTERS $ITER_NUM
  ;;
#  "30M_PR_GRX")
#      SOURCE_PATH=/data/pagerank/30M
#      OUTPUT_PATH=/pagerank/spark
#      ITER_NUM=10
#      E_PART=28
#
#      do_pagerank_graphx ${SOURCE_PATH} ${OUTPUT_PATH} ${ITER_NUM} ${E_PART}
#  ;;
  # Spark Terasort Jobs
  "2G_TERA_SPK")
      S_DIR=/data/terasort/2G-tera
      OUTPUT_HDFS=/output/tera/2G

      do_terasort_spk $S_DIR $OUTPUT_HDFS 1
  ;;
  "10G_TERA_SPK")
      S_DIR=/data/terasort/10G-tera
      OUTPUT_HDFS=/output/spark/tera

      do_terasort_spk $S_DIR $OUTPUT_HDFS 4
  ;;
  "20G_TERA_SPK")
      S_DIR=/data/terasort/20G-tera
      OUTPUT_HDFS=/output/spark/tera

      do_terasort_spk $S_DIR $OUTPUT_HDFS 4
  ;;
  "40G_TERA_SPK")
      S_DIR=/data/terasort/40G-tera
      OUTPUT_HDFS=/output/spark/tera

      do_terasort_spk $S_DIR $OUTPUT_HDFS 4
  ;;
  "80G_TERA_SPK")
      S_DIR=/data/terasort/80G-tera
      OUTPUT_HDFS=/output/spark/tera

      do_terasort_spk $S_DIR $OUTPUT_HDFS 4
  ;;
  "160G_TERA_SPK")
      S_DIR=/data/terasort/160G-tera
      OUTPUT_HDFS=/output/spark/tera

      do_terasort_spk $S_DIR $OUTPUT_HDFS 4
  ;;

  # Spark Sort Jobs
  "2G_ST_SPK")
      S_DIR=/data/text/2G-text
      OUTPUT_HDFS=/output/st/spark/2G

      do_text_sort_spk $S_DIR $OUTPUT_HDFS 1
  ;;
  "10G_ST_SPK")
      S_DIR=/data/text/10G-text
      OUTPUT_HDFS=/output/spark/st

      do_text_sort_spk $S_DIR $OUTPUT_HDFS 4
  ;;
  "20G_ST_SPK")
      S_DIR=/data/text/20G-text
      OUTPUT_HDFS=/output/spark/st

      do_text_sort_spk $S_DIR $OUTPUT_HDFS 4
  ;;
  "40G_ST_SPK")
      S_DIR=/data/text/40G-text
      OUTPUT_HDFS=/output/spark/st

      do_text_sort_spk $S_DIR $OUTPUT_HDFS 4
  ;;
  "80G_ST_SPK")
      S_DIR=/data/text/80G-text
      OUTPUT_HDFS=/output/spark/st

      do_text_sort_spk $S_DIR $OUTPUT_HDFS 4
  ;;
  "160G_ST_SPK")
      S_DIR=/data/text/160G-text
      OUTPUT_HDFS=/output/spark/st

      do_text_sort_spk $S_DIR $OUTPUT_HDFS 4
  ;;

  # Spark WordCount Jobs
  "2G_WC_SPK")
      S_DIR=/data/text/2G-text
      OUTPUT_HDFS=/output/wc

      do_text_wc_spk $S_DIR $OUTPUT_HDFS
  ;;
  "10G_WC_SPK")
      S_DIR=/data/text/10G-text
      OUTPUT_HDFS=/output/spark/wc

      do_text_wc_spk $S_DIR $OUTPUT_HDFS
  ;;
  "20G_WC_SPK")
      S_DIR=/data/text/20G-text
      OUTPUT_HDFS=/output/spark/wc

      do_text_wc_spk $S_DIR $OUTPUT_HDFS
  ;;
  "40G_WC_SPK")
      S_DIR=/data/text/40G-text
      OUTPUT_HDFS=/output/spark/wc

      do_text_wc_spk $S_DIR $OUTPUT_HDFS
  ;;
  "80G_WC_SPK")
      S_DIR=/data/text/80G-text
      OUTPUT_HDFS=/output/spark/wc

      do_text_wc_spk $S_DIR $OUTPUT_HDFS
  ;;
  "160G_WC_SPK")
      S_DIR=/data/text/160G-text
      OUTPUT_HDFS=/output/spark/wc

      do_text_wc_spk $S_DIR $OUTPUT_HDFS
  ;;

  # Flink Terasort Jobs
  "2G_TERA_FLK")
      S_DIR=/data/terasort/2G-tera
      OUTPUT_HDFS=/output/tera/2G

      do_terasort_flk $S_DIR $OUTPUT_HDFS 1
  ;;
  "10G_TERA_FLK")
      S_DIR=/data/terasort/10G-tera
      OUTPUT_HDFS=/output/flink/tera

      do_terasort_flk $S_DIR $OUTPUT_HDFS 4
  ;;
  "20G_TERA_FLK")
      S_DIR=/data/terasort/20G-tera
      OUTPUT_HDFS=/output/flink/tera

      do_terasort_flk $S_DIR $OUTPUT_HDFS 4
  ;;
  "40G_TERA_FLK")
      S_DIR=/data/terasort/40G-tera
      OUTPUT_HDFS=/output/flink/tera

      do_terasort_flk $S_DIR $OUTPUT_HDFS 4
  ;;
  "80G_TERA_FLK")
      S_DIR=/data/terasort/80G-tera
      OUTPUT_HDFS=/output/flink/tera

      do_terasort_flk $S_DIR $OUTPUT_HDFS 4
  ;;
  "160G_TERA_FLK")
      S_DIR=/data/terasort/160G-tera
      OUTPUT_HDFS=/output/flink/tera

      do_terasort_flk $S_DIR $OUTPUT_HDFS 4
  ;;

  # Flink WordCount Jobs
  "2G_WC_FLK")
      S_DIR=/data/text/2G-text
      OUTPUT_HDFS=/output/wc/2G

      do_text_wc_flk $S_DIR $OUTPUT_HDFS 1
  ;;
  "10G_WC_FLK")
      S_DIR=/data/text/10G-text
      OUTPUT_HDFS=/output/flink/wc

      do_text_wc_flk $S_DIR $OUTPUT_HDFS 4
  ;;
  "20G_WC_FLK")
      S_DIR=/data/text/20G-text
      OUTPUT_HDFS=/output/flink/wc

      do_text_wc_flk $S_DIR $OUTPUT_HDFS 4
  ;;
  "40G_WC_FLK")
      S_DIR=/data/text/40G-text
      OUTPUT_HDFS=/output/flink/wc

      do_text_wc_flk $S_DIR $OUTPUT_HDFS 4
  ;;
  "80G_WC_FLK")
      S_DIR=/data/text/80G-text
      OUTPUT_HDFS=/output/flink/wc

      do_text_wc_flk $S_DIR $OUTPUT_HDFS 4
  ;;
  "160G_WC_FLK")
      S_DIR=/data/text/160G-text
      OUTPUT_HDFS=/output/flink/wc

      do_text_wc_flk $S_DIR $OUTPUT_HDFS 4
  ;;

  # Flink Sort Jobs
  "2G_ST_FLK")
      S_DIR=/data/text/2G-text
      OUTPUT_HDFS=/output/st/flink/2G

      do_text_sort_flk $S_DIR $OUTPUT_HDFS 1
  ;;
  "10G_ST_FLK")
      S_DIR=/data/text/10G-text
      OUTPUT_HDFS=/output/flink/st

      do_text_sort_flk $S_DIR $OUTPUT_HDFS 4
  ;;
  "20G_ST_FLK")
      S_DIR=/data/text/20G-text
      OUTPUT_HDFS=/output/flink/st

      do_text_sort_flk $S_DIR $OUTPUT_HDFS 4
  ;;
  "40G_ST_FLK")
      S_DIR=/data/text/40G-text
      OUTPUT_HDFS=/output/flink/st

      do_text_sort_flk $S_DIR $OUTPUT_HDFS 4
  ;;
  "80G_ST_FLK")
      S_DIR=/data/text/80G-text
      OUTPUT_HDFS=/output/flink/st

      do_text_sort_flk $S_DIR $OUTPUT_HDFS 4
  ;;
  "160G_ST_FLK")
      S_DIR=/data/text/160G-text
      OUTPUT_HDFS=/output/flink/st

      do_text_sort_flk $S_DIR $OUTPUT_HDFS 4
  ;;

# self-test
  *)
    echo "Job undefined!"
  ;;
esac
