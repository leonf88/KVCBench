## Generate Data

### PageRank

Download the [SNAP tools][snap], use `krongen` to generate the data.
Here, use SNAP Release 3.0 as example.

    cd snap/examples/krongen
    make
    ./krongen -o:kronecker_graph.txt -m:"0.9 0.6; 0.6 0.1" -i:10

[snap]: https://snap.stanford.edu/snap/download.html

When the matrix is `0.9 0.6; 0.6 0.1`, the nodes and edges has the relationship with iterations as follows:

| Iterations | Nodes    | Edges     |
|------------|----------|-----------|
| 20         | 1048576  | 7054294   |
| 23         | 8388608  | 75114133  |
| 25         | 33554432 | 363552403 |

Because the MR model needs to process the data as multiple files and determine the task number.
We split the file to slices by

    split -l $((TOTAL_LINES/FILES_NUMBER))

### K-Means

K-Means data generator scripts usage:

    Usage: kmeans_gen.py [options]

    Options:
      -h, --help            show this help message and exit
      -w WORKER_SIZE, --worker=WORKER_SIZE
                            number of worker process
      -s FILE_SIZE, --filesize=FILE_SIZE
                            each slice file size in mega-bytes
      -n FILE_NUMBER, --filenum=FILE_NUMBER
                            number of slice files
      -d DIM, --dimension=DIM
                            dimension for each record
      -p DEST_PATH, --path=DEST_PATH
                            destination path
      --min=MIN_VALUE       maximum for each dimension
      --max=MAX_VALUE       minimum for each dimension


Using autogen to generate KMeans data

    INPUT_SAMPLE=/data/kmeans/1M/data
    INPUT_CLUSTER=/data/kmeans/1M/cluster
    NUM_OF_CLUSTERS=25
    NUM_OF_SAMPLES=1000000
    SAMPLES_PER_INPUTFILE=20000
    DIMENSIONS=100
    hadoop jar target/autogen-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.mahout.clustering.kmeans.GenKMeansDataset \
        -D hadoop.job.history.user.location=${INPUT_SAMPLE} \
        -sampleDir ${INPUT_SAMPLE} \
        -clusterDir ${INPUT_CLUSTER} \
        -numClusters ${NUM_OF_CLUSTERS} \
        -numSamples ${NUM_OF_SAMPLES} \
        -samplesPerFile ${SAMPLES_PER_INPUTFILE} \
        -sampleDimension ${DIMENSIONS}

P.S. the time will cost long, you can check the log on website to get the progress of the job like

    http://lingcloud21:18088/cluster/container/container_1488591290688_0007_01_000001
