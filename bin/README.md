
## Hadoop Commands

* PageRank

        /home/lf/workplace/BenchScripts/frameworks/hadoop-2.7.3/bin/hadoop \
            jar /home/lf/workplace/BenchScripts/hadoop-benchmarks/target/hadoop-benchmarks-1.0-SNAPSHOT.jar \
            pegasus.pagerank.PagerankNaive \
            /data/pagerank/10M \
            /output/pagerank/hadoop \
            8388608 28 10 \
            nosym new

* K-Means

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