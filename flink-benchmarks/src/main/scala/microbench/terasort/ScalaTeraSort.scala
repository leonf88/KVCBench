package microbench.terasort

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import scopt.OptionParser

class OptimizedFlinkTeraPartitioner(underlying: TotalOrderPartitioner) extends Partitioner[OptimizedText] {
  def partition(key: OptimizedText, numPartitions: Int): Int = {
    underlying.getPartition(key.getText)
  }
}


object ScalaTeraSort {

  case class Params(input: String = null,
                    output: String = null,
                    partitions: Int = 1)

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("ScalaTeraSort") {
      head("ScalaTeraSort: an example TeraSort app.")
      opt[Int]("partitions")
        .text(s"number of partitions, default; ${defaultParams.partitions}")
        .action((x, c) => c.copy(partitions = x))
      arg[String]("<input>")
        .text(s"input paths to examples")
        .required()
        .action((x, c) => c.copy(input = x))
      arg[String]("<output>")
        .text(s"output paths to examples")
        .required()
        .action((x, c) => c.copy(output = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.setParallelism(params.partitions)

    val mapredConf = new JobConf()
    // mapredConf.set("fs.defaultFS", hdfs)
    mapredConf.set("mapreduce.input.fileinputformat.inputdir", params.input)
    mapredConf.set("mapreduce.output.fileoutputformat.outputdir", params.output)
    mapredConf.setInt("mapreduce.job.reduces", params.partitions)

    val partitionFile = new Path(params.output, TeraInputFormat.PARTITION_FILENAME)
    val jobContext = Job.getInstance(mapredConf)
    TeraInputFormat.writePartitionFile(jobContext, partitionFile)
    val partitioner = new OptimizedFlinkTeraPartitioner(new TotalOrderPartitioner(mapredConf, partitionFile))

    env.readHadoopFile(new TeraInputFormat(), classOf[Text], classOf[Text], params.input)
      .map(tp => (new OptimizedText(tp._1), tp._2))
      .partitionCustom(partitioner, 0).sortPartition(0, Order.ASCENDING)
      .map(tp => (tp._1.getText, tp._2))
      .output(new HadoopOutputFormat[Text, Text](new TeraOutputFormat(), jobContext))
    env.execute("TeraSort")
  }
}


















