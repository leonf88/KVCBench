/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package mlbench.pagerank;

import mpi.MPI;
import mpi.MPIException;
import mpid.core.HadoopWriter;
import mpid.core.MPI_D;
import mpid.core.MPI_D_Exception;
import mpid.core.util.MPI_D_Constants;
import mpid.util.DataMPIUtil;
import mpid.util.hadoop.HadoopIOUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

/**
 * The Pagerank workload of DataMPI is transplanted from PEGASUS PagerankNaive.
 * STAGE 2: merge multiplication results. - Input: partial multiplication
 * results - Output: combined multiplication results
 */
@SuppressWarnings("deprecation")
public class PagerankMerge {

    private static final Log LOG = LogFactory.getLog(PagerankMerge.class);
    private static final String MAPRED_OUTPUT_DIR = "mapreduce.output.fileoutputformat.outputdir";
    private static String confPath = null;
    private static String inDir = null, outDir = null,
            maxUsedMemPercent = null, partSize = null, outFileNum = null,
            spillPercent = null;
    private static int number_nodes = 0;
    private static double mixing_c = 0;
    private static double random_coeff = 0;
    private static double converge_threshold = 0;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void main(String[] args) throws IOException,
            InterruptedException {
        try {
            parseArgs(args);
            HashMap<String, String> conf = new HashMap<String, String>();
            initConf(conf);
            MPI_D.Init(args, MPI_D.Mode.Common, conf);

            JobConf jobConf = new JobConf(confPath);
            if (MPI_D.COMM_BIPARTITE_O != null) {
                // O communicator
                int rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_O);
                int size = MPI_D.Comm_size(MPI_D.COMM_BIPARTITE_O);
                if (rank == 0) {
                    LOG.info(PagerankMerge.class.getSimpleName() + " O start.");
                }
                FileSplit[] inputs = DataMPIUtil.HDFSDataLocalLocator.getTaskInputs(
                        MPI_D.COMM_BIPARTITE_O, jobConf, inDir, rank);
                for (int i = 0; i < inputs.length; i++) {
                    FileSplit fsplit = inputs[i];
                    LineRecordReader kvrr = new LineRecordReader(jobConf, fsplit);

                    LongWritable key = kvrr.createKey();
                    Text value = kvrr.createValue();
                    {
                        while (kvrr.next(key, value)) {
                            String line_text = value.toString();
                            final String[] line = line_text.split("\t");
                            if (line.length >= 2) {
                                MPI_D.Send(new IntWritable(Integer.parseInt(line[0])), new Text(line[1]));
                            }
                        }
                    }
                }

            } else if (MPI_D.COMM_BIPARTITE_A != null) {
                // A communicator
                int rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_A);
                if (rank == 0) {
                    LOG.info(PagerankMerge.class.getSimpleName() + " A start.");
                }
                HadoopWriter<IntWritable, Text> outrw = HadoopIOUtil.getNewWriter(
                        jobConf, outDir, IntWritable.class, Text.class, TextOutputFormat.class,
                        null, rank, MPI_D.COMM_BIPARTITE_A);

                IntWritable oldKey = null;
                double next_rank = 0;
                double previous_rank = 0;
                double diff = 0;
                int local_diffs = 0;
                random_coeff = (1 - mixing_c) / (double) number_nodes;
                converge_threshold = ((double) 1.0 / (double) number_nodes) / 10;
                Object[] keyValue = MPI_D.Recv();
                while (keyValue != null) {
                    IntWritable key = (IntWritable) keyValue[0];
                    Text value = (Text) keyValue[1];
                    if (oldKey == null) {
                        oldKey = key;
                    }
                    if (!key.equals(oldKey)) {
                        next_rank = next_rank * mixing_c + random_coeff;
                        outrw.write(oldKey, new Text("v" + next_rank));
                        diff = Math.abs(previous_rank - next_rank);
                        if (diff > converge_threshold) {
                            local_diffs += 1;
                        }
                        oldKey = key;
                        next_rank = 0;
                        previous_rank = 0;
                    }

                    String cur_value_str = value.toString();
                    if (cur_value_str.charAt(0) == 's') {
                        previous_rank = Double.parseDouble(cur_value_str.substring(1));
                    } else {
                        next_rank += Double.parseDouble(cur_value_str.substring(1));
                    }

                    keyValue = MPI_D.Recv();
                }
                if (previous_rank != 0) {
                    next_rank = next_rank * mixing_c + random_coeff;
                    outrw.write(oldKey, new Text("v" + next_rank));
                    diff = Math.abs(previous_rank - next_rank);
                    if (diff > converge_threshold)
                        local_diffs += 1;
                }
                outrw.close();
                reduceDiffs(local_diffs, rank);
            }

            MPI_D.Finalize();
        } catch (MPI_D_Exception e) {
            e.printStackTrace();
        }
    }

    private static void initConf(HashMap<String, String> conf) {
        conf.put(MPI_D_Constants.ReservedKeys.KEY_CLASS,
                org.apache.hadoop.io.IntWritable.class.getName());
        conf.put(MPI_D_Constants.ReservedKeys.VALUE_CLASS,
                org.apache.hadoop.io.Text.class.getName());

        if (maxUsedMemPercent != null) {
            conf.put(MPI_D_Constants.ReservedKeys.CommonModeKeys.MAX_MEM_USED_PERCENT,
                    maxUsedMemPercent);
        }
        if (partSize != null) {
            conf.put(MPI_D_Constants.ReservedKeys.CommonModeKeys.BLOCK_SIZE, partSize);
        }
        if (outFileNum != null) {
            conf.put(MPI_D_Constants.ReservedKeys.CommonModeKeys.SEND_QUEUE_LENGTH, outFileNum);
        }
        if (spillPercent != null) {
            conf.put(MPI_D_Constants.ReservedKeys.CommonModeKeys.SPILL_PERCENT, spillPercent);
        }
    }

    private static void reduceDiffs(int local_diffs, int rank) {
        int diffs[] = {0};
        int bs[] = {local_diffs};
        try {
            MPI.COMM_WORLD.Reduce(bs, 0, diffs, 0, 1, MPI.INT, MPI.SUM, 0);
            MPI.COMM_WORLD.Barrier();
            if (rank == 0) {
                LOG.info("Uncoveraged diffs:" + diffs[0]);
                FileWriter output = new FileWriter("var.tmp");
                output.write(String.valueOf(diffs[0]));
                output.close();
                JobConf conf = new JobConf(confPath);
                final FileSystem fs = FileSystem.get(conf);
                fs.copyFromLocalFile(true, new Path("./var.tmp"), new Path("var.tmp"));
            }

        } catch (MPIException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void parseArgs(String[] args) {
        if (args.length < 5) {
            System.err.println("ERROR: Wrong number of parameters: "
                    + args.length + " instead of 5.");
            System.err.println("Usage: PagerankMerge <core-site-path> <inDir> <outDir> " +
                    "<number_nodes>");
            System.exit(-1);
        } else if (args.length == 5) {
            confPath = args[0];
            inDir = args[1];
            outDir = args[2];
            number_nodes = Integer.parseInt(args[3]);
            mixing_c = Double.parseDouble(args[4]);
        } else if (args.length <= 9) {
            // Advanced Usage
            confPath = args[0];
            inDir = args[1];
            outDir = args[2];
            number_nodes = Integer.parseInt(args[3]);
            mixing_c = Double.parseDouble(args[4]);
            maxUsedMemPercent = args[5];
            partSize = args[6];
            outFileNum = args[7];
            spillPercent = args[8];
        } else {
            System.err.println("ERROR: Error number of parameters.");
            System.exit(-1);
        }
    }
}
