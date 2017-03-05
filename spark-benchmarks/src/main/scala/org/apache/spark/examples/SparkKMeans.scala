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

// scalastyle:off println
package org.apache.spark.examples

import breeze.linalg.{DenseVector, squaredDistance}
import org.apache.hadoop.io.LongWritable
import org.apache.mahout.math.VectorWritable
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * K-means clustering.
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.ml.clustering.KMeans.
  */
object SparkKMeans {

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of KMeans Clustering and is given as an example!
        |Please use org.apache.spark.ml.clustering.KMeans
        |for more conventional use.
      """.stripMargin)
  }

  case class Params(input: String = null,
                    k: Int = -1,
                    numIterations: Int = 10)

  def closestPoint(p: DenseVector[Double], centers: Array[DenseVector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("SparkKMeans") {
      head("SparkKMeans: an example k-means app for dense data.")
      opt[Int]('k', "k")
        .required()
        .text(s"number of clusters, required")
        .action((x, c) => c.copy(k = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default; ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      arg[String]("<input>")
        .text("input paths to examples")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"Spark Kmeans")
    val sc = new SparkContext(conf)

    val data = sc.sequenceFile[LongWritable, VectorWritable](params.input)
    val examples = data.map { case (k, v) =>
      val vector: Array[Double] = new Array[Double](v.get().size)
      for (i <- 0 until v.get().size) vector(i) = v.get().get(i)
      DenseVector(vector)
    }.cache()

    val loadStartTime = System.currentTimeMillis()

    val kPoints = examples.takeSample(withReplacement = false, params.k, 42)
    var tempDist = 1.0

    for (i <- 0 until params.numIterations) {
      val t1 = System.currentTimeMillis()
      val closest = examples.map(p => (closestPoint(p, kPoints), (p, 1)))

      val pointStats = closest.reduceByKey { case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2) }

      val newPoints = pointStats.map { pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))
      }.collectAsMap()

      tempDist = 0.0
      for (j <- 0 until params.k) {
        if (newPoints.contains(j)) {
          tempDist += squaredDistance(kPoints(j), newPoints(j))
        }
      }

      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      val t2 = System.currentTimeMillis()
      printf("Finished iteration %d costs %.2f sec, (delta = %.2f ).\n", i, tempDist, (t2 - t1) / 1000.0f)
    }

    println("Final centers:")
    kPoints.foreach(println)

    sc.stop()
  }
}

// scalastyle:on println
