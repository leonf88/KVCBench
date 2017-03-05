package microbench

import org.apache.spark.{IOCommon, SparkConf, SparkContext}
import scopt.OptionParser

/*
 * Adopted from spark's example: https://spark.apache.org/examples.html
 */
object ScalaWordCount {

  case class Params(input: String = null,
                    output: String = null)

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("ScalaWordCount") {
      head("ScalaWordCount: an example WordCount app.")
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
    val sparkConf = new SparkConf().setAppName("ScalaWordCount")
    val sc = new SparkContext(sparkConf)

    val io = new IOCommon(sc)
    val data = io.load[String](params.input)
    val counts = data.flatMap(line => line.split("\\W+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    io.save(params.output, counts)
//    sc.stop()
  }
}
