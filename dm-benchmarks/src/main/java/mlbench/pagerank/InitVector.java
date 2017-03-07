/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package mlbench.pagerank;

import java.io.*;

public class InitVector {
	// Print the command-line usage text.
	protected static int printUsage() {
		System.out.println("InitVector <number_nodes>");
		return -1;
	}

	public static void main(final String[] args) throws IOException {
		if (args.length != 1) {
			printUsage();
			return;
		}

		int number_nodes = Integer.parseInt(args[0]);
		int i, j = 0;
		int milestone = number_nodes / 10;
		String file_name = "pagerank_init_vector.temp";
		FileWriter file = new FileWriter(file_name);
		BufferedWriter out = new BufferedWriter(file);

		System.out.print("Creating initial pagerank vectors...");
		double initial_rank = 1.0 / (double) number_nodes;

		for (i = 0; i < number_nodes; i++) {
			out.write(i + "\tv" + initial_rank + "\n");
			if (++j > milestone) {
				System.out.print(".");
				j = 0;
			}
		}
		out.close();
		System.out.println("");
	}
}
