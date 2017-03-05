/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package microbench;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * Implements the "Sort" program that computes a simple word occurrence histogram
 * over text files.
 * <p>
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 * <p>
 * <p>
 * Usage: <code>Sort --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * <p>
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 */
@SuppressWarnings("serial")
public class Sort {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataSet<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			throw new IOException("require --input configuration.");
		}

		DataSet<Tuple2<String, String>> sortedData =
			// split up the lines in pairs (2-tuples) containing: (word,sentence)
			text.flatMap(new Tokenizer())
				.sortPartition(0, Order.DESCENDING)
				.setParallelism(1);


		// emit result
		if (params.has("output")) {
			sortedData.writeAsCsv(params.get("output"), "\n", "\t");
			// execute program
			env.execute("Sort Example");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			sortedData.print();
		}

	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" ({@code Tuple2<String, Integer>}).
	 */
	private static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, String>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, String>> out) {
			// normalize and split the lin
			String[] strings = value.split("\\W+");
			out.collect(new Tuple2<String, String>(strings[0], strings[1]));
		}
	}

}
