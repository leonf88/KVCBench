package microbench

import java.util.StringTokenizer

import org.apache.spark.{IOCommon, SparkConf, SparkContext}

/*
 * Adopted from spark's example: https://spark.apache.org/examples.html
 */
object ScalaWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        s"Usage: $ScalaWordCount <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaWordCount")
    val sc = new SparkContext(sparkConf)

    val io = new IOCommon(sc)
    val data = io.load[String](args(0))
    val counts = data.flatMap(line => line.split("\\W+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    io.save(args(1), counts)
    sc.stop()
  }
}
