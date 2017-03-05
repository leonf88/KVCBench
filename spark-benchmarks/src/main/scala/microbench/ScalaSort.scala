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

package microbench

import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark._
import scopt.OptionParser


object ScalaSort {
  //  implicit def rddToHashedRDDFunctions[K: Ordering : ClassTag, V: ClassTag]
  //  (rdd: RDD[(K, V)]) = new ConfigurableOrderedRDDFunctions[K, V, (K, V)](rdd)

  implicit def ArrayByteOrdering: Ordering[Array[Byte]] = Ordering.fromLessThan {
    case (a, b) => (new BytesWritable(a).compareTo(new BytesWritable(b))) < 0
  }

  case class Params(input: String = null,
                    output: String = null,
                    partitions: Int = 1)

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("ScalaSort") {
      head(s"$ScalaSort: an example TeraSort app.")
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
    val sparkConf = new SparkConf().setAppName("ScalaSort")
    val sc = new SparkContext(sparkConf)

    val data = sc.newAPIHadoopFile[Text, Text, KeyValueTextInputFormat](params.input).map {
      case (k, v) => (k.copyBytes, v.copyBytes)
    }

    val partitioner = new HashPartitioner(partitions = params.partitions)
    val ordered_data = new ConfigurableOrderedRDDFunctions[Array[Byte], Array[Byte], (Array[Byte], Array[Byte])](data)
    val sorted_data = ordered_data.sortByKeyWithPartitioner(partitioner = partitioner).map {
      case (k, v) => (new Text(k), new Text(v))
    }

    sorted_data.saveAsNewAPIHadoopFile(params.output, classOf[Text], classOf[Text], classOf[TextOutputFormat[Text, Text]])
    sc.stop()
  }
}
