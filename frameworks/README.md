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

        ./configure --prefix=$HOME/opt/mvapich2 --with-device=ch3:nemesis \
        	    CFLAGS=-fPIC --disable-f77 --disable-fc
        make -j4 && make install

        rm -rf $HOME/opt/mpi
        ln -s $HOME/opt/mvapich2 $HOME/opt/mpi
        MPI_HOME=$HOME/opt/mpi
        PATH=$MPI_HOME/bin:$PATH
        LD_LIBRARY_PATH=$MPI_HOME/lib:$LD_LIBRARY_PATH
        export LD_LIBRARY_PATH

* Install mpich

        ./configure --prefix=$HOME/opt/mpich3 --with-device=ch3:nemesis \
            --enable-romio --enable-nemesis-dbg-localoddeven --enable-fast=O0 \
            CFLAGS=-fPIC --disable-fortran
        make -j4 && make install

        rm -rf $HOME/opt/mpi
        ln -s $HOME/opt/mpich3 $HOME/opt/mpi
        MPI_HOME=$HOME/opt/mpi
        PATH=$MPI_HOME/bin:$PATH
        LD_LIBRARY_PATH=$MPI_HOME/lib:$LD_LIBRARY_PATH
        export LD_LIBRARY_PATH

* Install DataMPI

        cmake -D CMAKE_INSTALL_PREFIX=$HOME/BenchScripts/frameworks/datampi-batch \
       	    -D MPI_D_BUILD_DOCS=OFF -D MPI_D_BUILD_TESTS=OFF \
       	    -D MPI_D_BUILD_EXAMPLES=OFF -D MPI_D_BUILD_BENCHMARKS=OFF \
       	    $HOME/BenchScripts/frameworks/DataMPI

        make install


### Download Others

Those binaries should be install in `$HOME/opt`

    wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u112-b15/jdk-8u112-linux-x64.tar.gz
    wget http://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.tgz
    wget https://archive.apache.org/dist/maven/binaries/apache-maven-3.2.2-bin.tar.gz

### AWS cluster setup

Request increase the limit

    Limit increase request 1
    Service: EC2 Instances
    Region: US West (Oregon)
    Primary Instance Type: t2.small
    Limit name: Instance Limit
    New limit value: 70
    ------------
    Use case description: Run experiments, need more instances to scale the test

Ubuntu prepare

    sudo apt-get install build-essential wget dstat

Linux prepare

    sudo yum groupinstall "Development Tools"

Update Network Security


| Type | Protocol | Port Range | Source |
| --- | --- | --- | --- |
| All TCP | TCP | 0 - 65535 | 172.31.0.0/16 |
| SSH | TCP | 22 | 0.0.0.0/0 |
| All UDP | TCP | 0 - 65535 | 172.31.0.0/16 |


Color Bash

    alias less='less --RAW-CONTROL-CHARS'
    export LS_OPTS='--color=auto'
    alias ls='ls ${LS_OPTS}'
