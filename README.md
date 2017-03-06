# BenchScripts
Benchmark scripts for DataMPI, Hadoop, Spark, Flink, etc.

## Compile the Benchmarks

    cd hadoop-benchmarks
    mvn clean package 
    
    cd flink-benchmarks
    mvn clean package assembly:single
    
    cd spark-benchmarks
    mvn clean package assembly:single

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