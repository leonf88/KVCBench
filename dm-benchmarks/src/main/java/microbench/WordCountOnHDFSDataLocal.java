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
import mpid.core.MPI_D_Combiner;
import mpid.core.MPI_D_Exception;
import mpid.core.util.MPI_D_Constants;
import mpid.util.DataMPIUtil;
import mpid.util.hadoop.HadoopIOUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * A word count example of DataMPI on HDFS with data-local feature.
 */
public class WordCountOnHDFSDataLocal {
    /*
     * Hadoop configuration path should be defined
     */
    private static String confPath = null;

    private static String inDir = null, outDir = null, maxUsedMemPercent = null, partSize = null,
            outFileNum = null, spillPercent = null;

    public static class WordCountCombiner implements MPI_D_Combiner<Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private List<IntWritable> results = new ArrayList<IntWritable>();

        @Override
        public Iterator<IntWritable> combine(Text key, Iterator<IntWritable> values)
                throws MPI_D_Exception {
            int sum = 0;
            results.clear();
            while (values.hasNext()) {
                sum += ((IntWritable) values.next()).get();
            }
            result.set(sum);
            results.add(result);
            return results.iterator();
        }

        public void close() {
            // do nothing
        }
    }

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

                HadoopReader<LongWritable, Text> reader = HadoopIOUtil.getReader(jobConf, inDir,
                        TextInputFormat.class,
                        rank, MPI_D.COMM_BIPARTITE_O);
                Text word = new Text();
                IntWritable one = new IntWritable(1);
                LongWritable khead = reader.createKey();
                Text vhead = reader.createValue();
                while (reader.next(khead, vhead)) {
                    StringTokenizer itr = new StringTokenizer(vhead.toString());
                    while (itr.hasMoreTokens()) {
                        word.set(itr.nextToken());
                        // send key-value
                        MPI_D.Send(word, one);
                    }
                }
                reader.close();
            } else if (MPI_D.COMM_BIPARTITE_A != null) {
                // A communicator
                int rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_A);
                int size = MPI_D.Comm_size(MPI_D.COMM_BIPARTITE_A);
                System.out.println("The A task " + rank + " of " + size + " is working...");

                HadoopWriter<Text, IntWritable> outrw = HadoopIOUtil.getNewWriter(jobConf, outDir,
                        Text.class, IntWritable.class, TextOutputFormat.class, null, rank,
                        MPI_D.COMM_BIPARTITE_A);

                Text oldKey = null;
                IntWritable valueData = new IntWritable();
                int sum = 0;
                Object[] keyValue = MPI_D.Recv();
                while (keyValue != null) {
                    Text key = (Text) keyValue[0];
                    IntWritable value = (IntWritable) keyValue[1];
                    if (oldKey == null) {
                        oldKey = key;
                        sum = value.get();
                    } else {
                        if (key.equals(oldKey)) {
                            sum += value.get();
                        } else {
                            valueData.set(sum);
                            outrw.write(oldKey, valueData);
                            oldKey = key;
                            sum = value.get();
                        }
                    }
                    keyValue = MPI_D.Recv();
                }
                if (oldKey != null) {
                    valueData.set(sum);
                    outrw.write(oldKey, valueData);
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
                org.apache.hadoop.io.IntWritable.class.getName());
        conf.put(MPI_D_Constants.ReservedKeys.COMBINER_CLASS, WordCountCombiner.class.getName());

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
            System.out.printf("ERROR: The %s example needs three parameters for HDFS conf path. e" +
                            ".g. core-site.xml, input and output direcories at least.\n",
                    WordCountOnHDFSDataLocal.class.getSimpleName());
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
