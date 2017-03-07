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

import mpid.core.HadoopWriter;
import mpid.core.MPI_D;
import mpid.core.MPI_D_Exception;
import mpid.core.util.MPI_D_Constants;
import mpid.util.DataMPIUtil;
import mpid.util.hadoop.HadoopIOUtil;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * The Pagerank workload of DataMPI is transplanted from PEGASUS PagerankNaive.
 * STAGE 1: Generate partial matrix-vector multiplication results. Perform hash
 * join using Vector.rowid == Matrix.colid. - Input: edge_file, pagerank vector
 * - Output: partial matrix-vector multiplication results.
 */
@SuppressWarnings("deprecation")
public class PagerankNaive {

    private static final Log LOG = LogFactory.getLog(PagerankNaive.class);
    private static final String MAPRED_OUTPUT_DIR = "mapreduce.output.fileoutputformat.outputdir";
    private static String confPath = null;
    private static String edgeDir = null, outDir = null, vecDir = null,
            maxUsedMemPercent = null, partSize = null, outFileNum = null,
            spillPercent = null;
    private static Integer make_symmetric = 0;

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
                    LOG.info(PagerankNaive.class.getSimpleName() + " O start.");
                }
                FileSplit[] inputs1 = DataMPIUtil.HDFSDataLocalLocator
                        .getTaskInputs(MPI_D.COMM_BIPARTITE_O, jobConf, edgeDir, rank);
                FileSplit[] inputs2 = DataMPIUtil.HDFSDataLocalLocator
                        .getTaskInputs(MPI_D.COMM_BIPARTITE_O, jobConf, vecDir, rank);
                FileSplit[] inputs = (FileSplit[]) ArrayUtils.addAll(inputs2, inputs1);
                for (int i = 0; i < inputs.length; i++) {
                    FileSplit fsplit = inputs[i];
                    LineRecordReader kvrr = new LineRecordReader(jobConf, fsplit);

                    LongWritable key = kvrr.createKey();
                    Text value = kvrr.createValue();
                    {
                        IntWritable k = new IntWritable();
                        Text v = new Text();
                        while (kvrr.next(key, value)) {
                            String line_text = value.toString();
                            // ignore comments in edge file
                            if (line_text.startsWith("#"))
                                continue;

                            final String[] line = line_text.split("\t");
                            if (line.length < 2)
                                continue;

                            // vector : ROWID VALUE('vNNNN')
                            if (line[1].charAt(0) == 'v') {
                                k.set(Integer.parseInt(line[0]));
                                v.set(line[1]);
                                MPI_D.Send(k, v);
                            } else {
                                /*
                                 * In other matrix-vector multiplication, we
								 * output (dst, src) here However, In PageRank,
								 * the matrix-vector computation formula is M^T
								 * * v. Therefore, we output (src,dst) here.
								 */
                                int src_id = Integer.parseInt(line[0]);
                                int dst_id = Integer.parseInt(line[1]);
                                k.set(src_id);
                                v.set(line[1]);
                                MPI_D.Send(k, v);

                                if (make_symmetric == 1) {
                                    k.set(dst_id);
                                    v.set(line[0]);
                                    MPI_D.Send(k, v);
                                }
                            }
                        }
                    }
                }

            } else if (MPI_D.COMM_BIPARTITE_A != null) {
                // A communicator
                int rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_A);
                if (rank == 0) {
                    LOG.info(PagerankNaive.class.getSimpleName() + " A start.");
                }

                HadoopWriter<IntWritable, Text> outrw = HadoopIOUtil.getNewWriter(
                        jobConf, outDir, IntWritable.class, Text.class, TextOutputFormat.class,
                        null, rank, MPI_D.COMM_BIPARTITE_A);

                IntWritable oldKey = null;
                int i;
                double cur_rank = 0;
                ArrayList<Integer> dst_nodes_list = new ArrayList<Integer>();
                Object[] keyValue = MPI_D.Recv();
                while (keyValue != null) {
                    IntWritable key = (IntWritable) keyValue[0];
                    Text value = (Text) keyValue[1];
                    if (oldKey == null) {
                        oldKey = key;
                    }
                    // A new key arrives
                    if (!key.equals(oldKey)) {
                        outrw.write(oldKey, new Text("s" + cur_rank));
                        int outdeg = dst_nodes_list.size();
                        if (outdeg > 0) {
                            cur_rank = cur_rank / (double) outdeg;
                        }
                        for (i = 0; i < outdeg; i++) {
                            outrw.write(new IntWritable(dst_nodes_list.get(i)),
                                    new Text("v" + cur_rank));
                        }
                        oldKey = key;
                        cur_rank = 0;
                        dst_nodes_list = new ArrayList<Integer>();
                    }
                    // common record
                    String line_text = value.toString();
                    final String[] line = line_text.split("\t");
                    if (line.length == 1) {
                        if (line_text.charAt(0) == 'v') { // vector : VALUE
                            cur_rank = Double.parseDouble(line_text.substring(1));
                        } else { // edge : ROWID
                            dst_nodes_list.add(Integer.parseInt(line[0]));
                        }
                    }
                    keyValue = MPI_D.Recv();
                }
                // write the left part
                if (cur_rank != 0) {
                    outrw.write(oldKey, new Text("s" + cur_rank));
                    int outdeg = dst_nodes_list.size();
                    if (outdeg > 0) {
                        cur_rank = cur_rank / (double) outdeg;
                    }
                    for (i = 0; i < outdeg; i++) {
                        outrw.write(
                                new IntWritable(dst_nodes_list.get(i)), new Text("v" + cur_rank));
                    }
                }
                outrw.close();
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

    private static void parseArgs(String[] args) {
        if (args.length < 4) {
            System.err.println("ERROR: Wrong number of parameters: " + args.length +
                    " instead of 3.");
            System.err.println("Usage: PagerankNaive <core-site-path> <edgeDir><vecDir> <outDir>");
            System.exit(-1);
        } else if (args.length == 4) {
            confPath = args[0];
            edgeDir = args[1];
            vecDir = args[2];
            outDir = args[3];
        } else if (args.length <= 8) {
            // Advanced Usage
            confPath = args[0];
            edgeDir = args[1];
            vecDir = args[2];
            outDir = args[3];
            maxUsedMemPercent = args[4];
            partSize = args[5];
            outFileNum = args[6];
            spillPercent = args[7];
        } else {
            System.err.println("ERROR: Error number of parameters.");
            System.exit(-1);
        }
    }
}
