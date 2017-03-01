## Generate Data

### PageRank

Download the [SNAP tools][snap], use `krongen` to generate the data.
Here, use SNAP Release 3.0 as example.

    cd snap/examples/krongen
    make
    krongen -o:kronecker_graph.txt -m:"0.9 0.6; 0.6 0.1" -i:10

[snap]: https://snap.stanford.edu/snap/download.html

### Kmeans

