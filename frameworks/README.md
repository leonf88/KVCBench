## Install the frameworks to bench

Download the frameworks with `bash setup.sh`, including

* Hadoop
* Spark
* Flink

### Hadoop

    # format the directories
    bin/hadoop namenode -format
    # start dfs
    sbin/start-dfs.sh

Check the log and web page to make sure the HDFS is start correctly

`$HADOOP_HOME/logs/hadoop-lf-namenode-lingcloud21.log`
`http://lingcloud21:9001/dfshealth.html#tab-datanode`

### Flink

### Spark

### DataMPI/iDataMPI