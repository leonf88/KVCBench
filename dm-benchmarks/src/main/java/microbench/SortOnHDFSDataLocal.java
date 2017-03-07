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

package microbench;

import mpid.core.HadoopReader;
import mpid.core.HadoopWriter;
import mpid.core.MPI_D;
import mpid.core.MPI_D_Exception;
import mpid.core.util.MPI_D_Constants;
import mpid.util.DataMPIUtil;
import mpid.util.hadoop.HadoopIOUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;

/**
 * A sort example of DataMPI on HDFS with data-local feature.
 */
public class SortOnHDFSDataLocal {
    /*
     * Hadoop configuration path should be defined
     */
    private static String confPath = null;

    private static String inDir = null, outDir = null, maxUsedMemPercent = null, partSize = null,
            outFileNum = null, spillPercent = null;

    public static void main(String[] args) throws IOException, InterruptedException {
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
                    DataMPIUtil.printArgs(args);
                }
                System.out.println("The O task " + rank + " of " + size + " is working...");

                HadoopReader<Text, Text> reader = HadoopIOUtil.getReader(jobConf, inDir,
                        KeyValueTextInputFormat.class, rank, MPI_D.COMM_BIPARTITE_O);
                Text khead = reader.createKey();
                Text vhead = reader.createValue();
                while (reader.next(khead, vhead)) {
                    MPI_D.Send(khead, vhead);
                }
                reader.close();
            } else if (MPI_D.COMM_BIPARTITE_A != null) {
                // A communicator
                int rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_A);
                int size = MPI_D.Comm_size(MPI_D.COMM_BIPARTITE_A);
                System.out.println("The A task " + rank + " of " + size + " is working...");

                HadoopWriter<Text, Text> outrw = HadoopIOUtil.getNewWriter(jobConf, outDir,
                        Text.class, Text.class, TextOutputFormat.class, null, rank,
                        MPI_D.COMM_BIPARTITE_A);
                // recv key-value
                Object[] keyValue = MPI_D.Recv();
                while (keyValue != null) {
                    outrw.write((Text) keyValue[0], (Text) keyValue[1]);
                    keyValue = MPI_D.Recv();
                }
                outrw.close();
            }
            MPI_D.Finalize();
        } catch (MPI_D_Exception e) {
            e.printStackTrace();
        }
    }

    private static void initConf(HashMap<String, String> conf) {
        conf.put(MPI_D_Constants.ReservedKeys.KEY_CLASS, org.apache.hadoop.io.Text.class.getName());
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
        if (args.length < 3) {
            System.err.println("ERROR: The "
                    + SortOnHDFSDataLocal.class.getSimpleName()
                    + " example needs three parameters"
                    + " for HDFS conf path. e.g. core-site.xml, input and output direcories at least.");
            System.exit(-1);
        } else if (args.length == 3) {
            confPath = args[0];
            inDir = args[1];
            outDir = args[2];
        } else if (args.length <= 7) {
            // Advanced Usage
            confPath = args[0];
            inDir = args[1];
            outDir = args[2];
            maxUsedMemPercent = args[3];
            partSize = args[4];
            outFileNum = args[5];
            spillPercent = args[6];
        } else {
            System.err.println("ERROR: Error number of parameters.");
            System.exit(-1);
        }
    }

}
