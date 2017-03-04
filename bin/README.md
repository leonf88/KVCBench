
## Hadoop Commands

* PageRank

* K-Means

    /home/lf/workplace/BenchScripts/frameworks/apache-mahout-distribution-0.12.2/bin/mahout kmeans \
        -i /data/kmeans/10M/data \
        -c /data/kmeans/10M/cluster \
        -o /output/kmeans/hadoop/10M \
        -x 10 \
        -dm org.apache.mahout.common.distance.EuclideanDistanceMeasure \
        -ow \
        -cl \
        -cd 0.5 \
        -xm mapreduce

## Start spark

    sbin/start-all.sh