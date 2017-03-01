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



