package microbench

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import scopt.OptionParser

class Tokenizer extends FlatMapFunction[String, Tuple2[String, String]] {
  override def flatMap(t: String, collector: Collector[(String, String)]): Unit = {
    val strings = t.split("\t")
    if (strings.length > 1) {
      collector.collect(Tuple2(strings(0), strings(1)))
    }
  }
}

object ScalaSort {

  case class Params(input: String = null,
                    output: String = null,
                    partitions: Int = 1)

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("ScalaSort") {
      head(s"$ScalaSort: an example Sort app.")
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

    val counts = env.readTextFile(params.input).flatMap(new Tokenizer())
      .sortPartition(0, Order.DESCENDING)

    counts.writeAsCsv(params.output, "\n", "\t")
    env.execute("Scala Sort Example")
  }
}
