## Install the frameworks to bench

Download the frameworks with `bash setup.sh`, including

* Hadoop
* Spark
* Flink
* Mahout
* Mpich

### Hadoop

    # format the directories
    bin/hadoop namenode -format
    # start dfs
    sbin/start-dfs.sh

Check the log and web page to make sure the HDFS is start correctly

    $HADOOP_HOME/logs/hadoop-lf-namenode-lingcloud21.log
    http://lingcloud21:9001/dfshealth.html#tab-datanode

### Flink

### Spark

*  Compile Spark with scala 2.11

        export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
        ./dev/change-scala-version.sh 2.11
        mvn -Pyarn -Phadoop-2.6 -Dscala-2.11 -DskipTests clean package
        ./make-distribution.sh --tgz  -Phadoop-2.6 -Pyarn -Pscala-2.11 -PskipTests

### DataMPI

* Install mvapich (seems unstable)

        ./configure --prefix=/home/lf/opt/mvapich2 --with-device=ch3:nemesis \
        	    CFLAGS=-fPIC --disable-f77 --disable-fc
        make -j4 && make install

        rm -rf /home/lf/opt/mpi
        ln -s /home/lf/opt/mvapich2 /home/lf/opt/mpi
        MPI_HOME=/home/lf/opt/mpi
        PATH=$MPI_HOME/bin:$PATH
        LD_LIBRARY_PATH=$MPI_HOME/lib:$LD_LIBRARY_PATH
        export LD_LIBRARY_PATH

* Install mpich

        ./configure --prefix=/home/lf/opt/mpich3 --with-device=ch3:nemesis \
            --enable-romio --enable-nemesis-dbg-localoddeven --enable-fast=O0 \
            CFLAGS=-fPIC --disable-fortran
        make -j4 && make install

        rm -rf /home/lf/opt/mpi
        ln -s /home/lf/opt/mpich3 /home/lf/opt/mpi
        MPI_HOME=/home/lf/opt/mpi
        PATH=$MPI_HOME/bin:$PATH
        LD_LIBRARY_PATH=$MPI_HOME/lib:$LD_LIBRARY_PATH
        export LD_LIBRARY_PATH

* Install DataMPI

        cmake -D CMAKE_INSTALL_PREFIX=/home/lf/workplace/BenchScripts/frameworks/datampi-batch \
       	    -D MPI_D_BUILD_DOCS=OFF -D MPI_D_BUILD_TESTS=OFF \
       	    -D MPI_D_BUILD_EXAMPLES=OFF -D MPI_D_BUILD_BENCHMARKS=OFF \
       	    /home/lf/workplace/BenchScripts/frameworks/DataMPI

        make install


