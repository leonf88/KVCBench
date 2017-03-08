## Hadoop Commands

* TeraSort Validate

        /home/lf/workplace/BenchScripts/frameworks/hadoop-2.7.3/bin/hadoop \
            jar /home/lf/workplace/BenchScripts/frameworks/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar \
            teravalidate /output/tera/2G /output/tera/validate

* PageRank

        /home/lf/workplace/BenchScripts/frameworks/hadoop-2.7.3/bin/hadoop \
            jar /home/lf/workplace/BenchScripts/hadoop-benchmarks/target/hadoop-benchmarks-1.0-SNAPSHOT.jar \
            pegasus.pagerank.PagerankNaive \
            /data/pagerank/10M \
            /output/pagerank/hadoop \
            8388608 28 10 \
            nosym new

* K-Means

    (Deprecation) Prepare the Sequence File (generate data from CSV, please see data-generator/README.md)

        /home/lf/workplace/BenchScripts/frameworks/apache-mahout-distribution-0.12.2/bin/mahout seqdirectory \
            -i /data/kmeans/data_kddcup04/raw \
            -o /data/kmeans/data_kddcup04/data

    Run Clustering Algorithm

        /home/lf/workplace/BenchScripts/frameworks/apache-mahout-distribution-0.12.2/bin/mahout kmeans \
            -i /data/kmeans/10M/data \
            -c /data/kmeans/10M/cluster \
            -o /output/kmeans/hadoop/10M \
            -x 10 \
            -dm org.apache.mahout.common.distance.EuclideanDistanceMeasure \
            -ow \
            -cd 0.5 \
            -xm mapreduce

## Spark Commands

* PageRank

        /home/lf/workplace/BenchScripts/frameworks/spark-1.6.2-bin-hadoop2.6/bin/spark-submit \
            --class org.apache.spark.examples.SparkPageRank \
            --properties-file /home/lf/workplace/BenchScripts/frameworks/spark- 1.6.2-bin-hadoop2.6/conf/spark-defaults.conf \
            --master spark://172.22.1.21:7077 \
            /home/lf/workplace/BenchScripts/spark-benchmarks/target/spark-benchmarks-1.0-SNAPSHOT.jar \
            --numIterations 10 \
            hdfs://172.22.1.21:9003//data/pagerank/10M \
            hdfs://172.22.1.21:9003//output/pagerank/spark

* K-Means

        export SPARKBENCH_PROPERTIES_FILES=/home/lf/workplace/BenchScripts/bin/conf/bench-txt.conf
        # or
        export SPARKBENCH_PROPERTIES_FILES=/home/lf/workplace/BenchScripts/bin/conf/bench-seq.conf

        /home/lf/workplace/BenchScripts/frameworks/spark-1.6.2-bin-hadoop2.6/bin/spark-submit \
            --class org.apache.spark.examples.SparkKMeans \
            --properties-file /home/lf/workplace/BenchScripts/frameworks/spark-1.6.2-bin-hadoop2.6/conf/spark-defaults.conf \
            --master spark://172.22.1.21:7077 \
            /home/lf/workplace/BenchScripts/spark-benchmarks/target/spark-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar \
            --k 25 \
            --numIterations 10 \
            hdfs://172.22.1.21:9003//data/kmeans/10M/data

* TeraSort

        /home/lf/workplace/BenchScripts/frameworks/spark-1.6.2-bin-hadoop2.6/bin/spark-submit \
            --class microbench.ScalaTeraSort \
            --properties-file /home/lf/workplace/BenchScripts/frameworks/spark-1.6.2-bin-hadoop2.6/conf/spark-defaults.conf \
            --master spark://172.22.1.21:7077 \
            /home/lf/workplace/BenchScripts/spark-benchmarks/target/spark-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar \
            --partitions 7 \
            hdfs://172.22.1.21:9003//data/terasort/2G-tera hdfs://172.22.1.21:9003//output/tera/2G

* WordCount

        /home/lf/workplace/BenchScripts/frameworks/spark-1.6.2-bin-hadoop2.6/bin/spark-submit \
            --class microbench.ScalaWordCount \
            --properties-file /home/lf/workplace/BenchScripts/frameworks/spark-1.6.2-bin-hadoop2.6/conf/spark-defaults.conf \
            --master spark://172.22.1.21:7077 \
            /home/lf/workplace/BenchScripts/spark-benchmarks/target/spark-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar \
            hdfs://172.22.1.21:9003//data/terasort/2G-tera hdfs://172.22.1.21:9003//output/tera/2Gt

* Sort

        /home/lf/workplace/BenchScripts/frameworks/spark-1.6.2-bin-hadoop2.6/bin/spark-submit \
            --class microbench.ScalaSort \
            --properties-file /home/lf/workplace/BenchScripts/frameworks/spark-1.6.2-bin-hadoop2.6/conf/spark-defaults.conf \
            --master spark://172.22.1.21:7077 \
            /home/lf/workplace/BenchScripts/spark-benchmarks/target/spark-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar \
            --partitions 7 \
            hdfs://172.22.1.21:9003//data/text/2G-text hdfs://172.22.1.21:9003//output/st/2G

## Flink Commands

* TeraSort

        /home/lf/workplace/BenchScripts/frameworks/flink-1.1.2/bin/flink run \
            -c microbench.terasort.ScalaTeraSort \
            /home/lf/workplace/BenchScripts/flink-benchmarks/target/flink-benchmarks-1.0-SNAPSHOT.jar \
            --partitions 28 \
            hdfs://172.22.1.21:9003/data/terasort/2G-tera hdfs://172.22.1.21:9003/output/tera/2G

* WordCount

        /home/lf/workplace/BenchScripts/frameworks/flink-1.1.2/bin/flink run \
            -c microbench.ScalaWordCount \
            /home/lf/workplace/BenchScripts/flink-benchmarks/target/flink-benchmarks-1.0-SNAPSHOT.jar \
            --partitions 7 \
            hdfs://172.22.1.21:9003//data/text/2G-text hdfs://172.22.1.21:9003//output/wc

* Sort

        /home/lf/workplace/BenchScripts/frameworks/flink-1.1.2/bin/flink run \
            -c microbench.ScalaSort \
            /home/lf/workplace/BenchScripts/flink-benchmarks/target/flink-benchmarks-1.0-SNAPSHOT.jar \
            --partitions 7 \
            hdfs://172.22.1.21:9003//data/text/2G-text hdfs://172.22.1.21:9003//output/st

## DataMPI Commands

* TeraSort

        /home/lf/workplace/BenchScripts/frameworks/datampi-batch/bin/mpidrun \
            -f /home/lf/workplace/BenchScripts/frameworks/datampi-batch/conf/hostfile \
            -mode COM -O 1 -A 1 \
            -jar /home/lf/workplace/BenchScripts/dm-benchmarks/target/dm-benchmarks-1.0-SNAPSHOT.jar \
            microbench.TeraSortOnHDFSDataLocal \
            /home/lf/workplace/BenchScripts/frameworks/hadoop-2.7.3/etc/hadoop/core-site.xml \
            /data/terasort/2G-tera /output/tera/2G

* WordCount

        /home/lf/workplace/BenchScripts/frameworks/datampi-batch/bin/mpidrun \
            -f /home/lf/workplace/BenchScripts/frameworks/datampi-batch/conf/hostfile \
            -mode COM -O 1 -A 1 \
            -jar /home/lf/workplace/BenchScripts/dm-benchmarks/target/dm-benchmarks-1.0-SNAPSHOT.jar \
            microbench.WordCountOnHDFSDataLocal \
            /home/lf/workplace/BenchScripts/frameworks/hadoop-2.7.3/etc/hadoop/core-site.xml \
            /data/text/2G-text /output/text/wc

* KMeans

    Initialize Centers

        /home/lf/workplace/BenchScripts/frameworks/datampi-batch/bin/mpidrun \
            -f /home/lf/workplace/BenchScripts/frameworks/datampi-batch/conf/hostfile \
            -mode COM -O 1 -A 1 \
            -jar /home/lf/workplace/BenchScripts/dm-benchmarks/target/dm-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar \
            mlbench.kmeans.KmeansInit \
            /home/lf/workplace/BenchScripts/frameworks/hadoop-2.7.3/etc/hadoop/core-site.xml \
            /data/kmeans/data_kddcup04/data /output/kmeans/kdd-dm/center0 25

    Iteration

        /home/lf/workplace/BenchScripts/frameworks/datampi-batch/bin/mpidrun \
            -f /home/lf/workplace/BenchScripts/frameworks/datampi-batch/conf/hostfile \
            -mode COM -O 1 -A 1 \
            -jar /home/lf/workplace/BenchScripts/dm-benchmarks/target/dm-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar \
            mlbench.kmeans.KmeansIter \
            /home/lf/workplace/BenchScripts/frameworks/hadoop-2.7.3/etc/hadoop/core-site.xml \
            /data/kmeans/data_kddcup04/data /output/kmeans/kdd-dm/center0 /output/kmeans/kdd-dm/out1 25

* PageRank

    Initialize Vectors

        DMB_JAR=/home/lf/workplace/BenchScripts/dm-benchmarks/target/dm-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar
        java -cp $DMB_JAR mlbench.pagerank.InitVector $((2**20))
        hadoop fs -mkdir /data/pagerank/1M_dm_init_vector
        hadoop fs -put pagerank_init_vector.temp /data/pagerank/1M_dm_init_vector

    Iteration

        /home/lf/workplace/BenchScripts/frameworks/datampi-batch/bin/mpidrun \
            -f /home/lf/workplace/BenchScripts/frameworks/datampi-batch/conf/hostfile \
            -mode COM -O 1 -A 1 \
            -jar /home/lf/workplace/BenchScripts/dm-benchmarks/target/dm-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar \
            mlbench.pagerank.PagerankNaive \
            /home/lf/workplace/BenchScripts/frameworks/hadoop-2.7.3/etc/hadoop/core-site.xml \
            /data/pagerank/1M /pagerank/mpid/pr_temp_vec /pagerank/mpid/pr_temp_model