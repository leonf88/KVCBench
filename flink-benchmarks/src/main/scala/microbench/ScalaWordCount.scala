package microbench

import org.apache.flink.api.scala._
import scopt.OptionParser


object ScalaWordCount {

  case class Params(input: String = null,
                    output: String = null,
                    partitions: Int = 1)

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("ScalaWordCount") {
      head(s"$ScalaWordCount: an example WordCount app.")
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

    val counts = env.readTextFile(params.input).flatMap(
      _.split("\\W+") filter {
        _.nonEmpty
      })
      .map {
        (_, 1)
      }
      .groupBy(0)
      .sum(1)

    counts.writeAsCsv(params.output, "\n", " ")
    env.execute("Scala WordCount Example")
  }
}
