/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copied from org.apache.spark.examples.JavaPageRank
 * Modification from origin:
 *    Use saveAsText instead of print to present the result. See the commented
 * code at the tail of the code.
 */

package org.apache.spark.examples

import org.apache.spark.SparkContext._
import org.apache.spark.{IOCommon, SparkConf, SparkContext}
import scopt.OptionParser

/**
  * Computes the PageRank of URLs from an input file. Input file should
  * be in format of:
  * URL         neighbor URL
  * URL         neighbor URL
  * URL         neighbor URL
  * ...
  * where URL and their neighbors are separated by space(s).
  */
object SparkPageRank {

  case class Params(input: String = null,
                    output: String = null,
                    numIterations: Int = 10)

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("SparkKPageRank") {
      head("SparkKPageRank: an example PageRank app.")
      opt[Int]("numIterations")
        .text(s"number of iterations, default; ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      arg[String]("<input>")
        .text(s"input paths to examples")
        .required()
        .action((x, c) => c.copy(input = x))
      opt[String]("<output>")
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
    val sparkConf = new SparkConf().setAppName("SparkPageRank")
    val ctx = new SparkContext(sparkConf)

    //  Modified by Lv: accept last two values from HiBench generated PageRank data format
    val lines = ctx.textFile(params.input, 1)
    val links = lines.map { s =>
      val elements = s.split("\\s+")
      val parts = elements.slice(elements.length - 2, elements.length)
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to params.numIterations) {
      val startT = System.currentTimeMillis()
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      val stopT = System.currentTimeMillis()
      printf("Finished iteration %d costs %.2f sec.\n", i, (stopT - startT) / 1000.0f)
    }

    //    val output = ranks.collect()
    //    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    val io = new IOCommon(ctx)
    io.save(params.output, ranks)
    //    ranks.saveAsTextFile(output_path)

    ctx.stop()
  }
}
