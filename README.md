# BenchScripts
Benchmark scripts for DataMPI, Hadoop, Spark, Flink, etc.

## Compile the Benchmarks

    cd hadoop-benchmarks
    mvn clean package 
    
    cd flink-benchmarks
    mvn clean package assembly:single
    
    cd spark-benchmarks
    mvn clean package assembly:single

    cd dm-benchmarks
    mvn clean package -Dmaven.test.skip=true

## Exception

### 1. Compile Spark with scala 2.11

When run spark-1.6.2, it will throw the exception

    Exception in thread "main" java.lang.NoSuchMethodError: scala.runtime.ObjectRef.create(Ljava/lang/Object;)Lscala/runtime/ObjectRef;
    
The root cause is that local scala is 2.11.x, but the spark binary of cluster was compiled by 2.10 scala.
So, the solution is to compile the spark using 2.11.x scala libraries or use 2.10 scala to run spark.

    export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
    ./dev/change-scala-version.sh 2.11
    mvn -Pyarn -Phadoop-2.6 -Dscala-2.11 -DskipTests clean package
    ./make-distribution.sh --tgz  -Phadoop-2.6 -Pyarn -Pscala-2.11 -PskipTests

### 2. HDFS Slow

    17/03/07 18:47:24 WARN hdfs.DFSClient: Slow ReadProcessor read fields took 31013ms (threshold=30000ms); ack: seqno: 3312 status: SUCCESS status: SUCCESS status: SUCCESS downstreamAckTimeNanos: 2479577 4: "\000\000\000", targets: [172.22.1.22:50010, 172.22.1.26:50010, 172.22.1.24:50010]


dfs.datanode.handler.count（加大）DN的服务线程数。这些线程仅用于接收请求，处理业务命令
dfs.namenode.handler.count（加大）  NN的服务线程数。用于处理RPC请求
dfs.namenode.avoid.read.stale.datanode（true）决定是否避开从脏DN上读数据。脏DN指在一个指定的时间间隔内没有收到心跳信息。脏DN将被移到可以读取(写入)节点列表的尾端。尝试开启
dfs.namenode.avoid.write.stale.datanode（true）  和上面相似，是为了避免向脏DN写数据