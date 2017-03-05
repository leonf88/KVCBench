package microbench

import org.apache.flink.api.scala._


object WordCount2 {

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        s"Usage: $WordCount inputPath outputPath"
      )
      System.exit(1)
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()

    val hdfs = args(0)
    val inputPath = hdfs + args(1)
    val outputPath = hdfs + args(2)
    val counts = env.readTextFile(inputPath).flatMap(
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      })
      .map {
        (_, 1)
      }
      .groupBy(0)
      .sum(1)

    counts.writeAsCsv(outputPath, "\n", " ")
    env.execute("Scala WordCount Example")
  }
}
