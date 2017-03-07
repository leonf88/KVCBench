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
package mlbench.bayes.train;

import mlbench.bayes.BayesUtils;
import mpi.MPIException;
import mpid.core.MPI_D;
import mpid.core.MPI_D_Combiner;
import mpid.core.MPI_D_Exception;
import mpid.core.util.MPI_D_Constants;
import mpid.util.DataMPIUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;


public class IndexInstances {
    private static Configuration config;
    private static final String MAPRED_OUTPUT_DIR = "mapred.output.dir";
    private static String confPath = null;
    private static String inDir = null, outDir = null, labDir = null,
            maxUsedMemPercent = null, partSize = null, outFileNum = null,
            spillPercent = null;
    private static Path inPath = null, labPath = null;
    private static int rank = -1;

    public static class IndexInstancesCombiner implements
            MPI_D_Combiner<WritableComparable<?>, VectorWritable> {
        private VectorWritable result = new VectorWritable();
        private List<VectorWritable> results = new ArrayList<VectorWritable>();

        @Override
        public Iterator<VectorWritable> combine(WritableComparable<?> arg0,
                                                Iterator<VectorWritable> values) throws
                MPI_D_Exception {
            Vector vector = null;
            while (values.hasNext()) {
                VectorWritable v = (VectorWritable) values.next();
                if (vector == null) {
                    vector = v.get();
                } else {
                    vector.assign(v.get(), Functions.PLUS);
                }
            }
            results.clear();
            result.set(vector);
            results.add(result);
            return results.iterator();
        }

        public void close() {
        }

    }

    @SuppressWarnings({"deprecation"})
    public static void main(String[] args) throws MPI_D_Exception, IOException,
            MPIException {
        parseArgs(args);
        HashMap<String, String> conf = new HashMap<String, String>();
        initConf(conf);
        MPI_D.Init(args, MPI_D.Mode.Common, conf);
        if (MPI_D.COMM_BIPARTITE_O != null) {
            rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_O);

            if (rank == 0) {
                System.out.println(IndexInstances.class.getSimpleName()
                        + " O start.");
                createLabelIndex(labPath);
            }

            HadoopUtil.cacheFiles(labPath, config);

            MPI_D.COMM_BIPARTITE_O.Barrier();

            OpenObjectIntHashMap<String> labelIndex = BayesUtils
                    .readIndexFromCache(config);

            if (MPI_D.COMM_BIPARTITE_O != null) {
                // O communicator
                int rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_O);
                int size = MPI_D.Comm_size(MPI_D.COMM_BIPARTITE_O);
                FileSplit[] inputs = DataMPIUtil.HDFSDataLocalLocator.getTaskInputs(
                        MPI_D.COMM_BIPARTITE_O, (JobConf) config, inDir, rank);
                for (int i = 0; i < inputs.length; i++) {
                    FileSplit fsplit = inputs[i];
                    SequenceFileRecordReader<Text, VectorWritable> kvrr = new
                            SequenceFileRecordReader<>(config, fsplit);
                    Text labelText = kvrr.createKey();
                    VectorWritable instance = kvrr.createValue();
                    while (kvrr.next(labelText, instance)) {
                        String label = SLASH.split(labelText.toString())[1];
                        if (labelIndex.containsKey(label)) {
                            MPI_D.Send(new IntWritable(labelIndex.get(label)), instance);
                        }
                    }
                }
            }
        } else if (MPI_D.COMM_BIPARTITE_A != null) {
            int rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_A);
            config.set(MAPRED_OUTPUT_DIR, outDir);
            config.set("mapred.task.id", DataMPIUtil.getHadoopTaskAttemptID()
                    .toString().toString());
            ((JobConf) config).setOutputKeyClass(IntWritable.class);
            ((JobConf) config).setOutputValueClass(VectorWritable.class);
            TaskAttemptContext taskContext = new TaskAttemptContextImpl(config,
                    DataMPIUtil.getHadoopTaskAttemptID());
            SequenceFileOutputFormat<IntWritable, VectorWritable> outfile = new
                    SequenceFileOutputFormat<>();
            FileSystem fs = FileSystem.get(config);

            Path output = new Path(config.get(MAPRED_OUTPUT_DIR));
            FileOutputCommitter fcommitter = new FileOutputCommitter(output,
                    taskContext);
            RecordWriter<IntWritable, VectorWritable> outrw = null;
            try {
                fcommitter.setupJob(taskContext);
                outrw = outfile.getRecordWriter(fs, (JobConf) config,
                        getOutputName(rank), null);
            } catch (IOException e) {
                e.printStackTrace();
                System.err
                        .println("ERROR: Please set the HDFS configuration properly\n");
                System.exit(-1);
            }

            IntWritable key = null, newKey = null;
            VectorWritable point = null, newPoint = null;
            Vector vector = null;
            Object[] vals = MPI_D.Recv();
            while (vals != null) {
                newKey = (IntWritable) vals[0];
                newPoint = (VectorWritable) vals[1];
                if (key == null && point == null) {
                } else if (!key.equals(newKey)) {
                    outrw.write(key, new VectorWritable(vector));
                    vector = null;
                }
                if (vector == null) {
                    vector = newPoint.get();
                } else {
                    vector.assign(newPoint.get(), Functions.PLUS);
                }

                key = newKey;
                point = newPoint;
                vals = MPI_D.Recv();
            }
            if (newKey != null && newPoint != null) {
                outrw.write(key, new VectorWritable(vector));
            }

            outrw.close(null);
            if (fcommitter.needsTaskCommit(taskContext)) {
                fcommitter.commitTask(taskContext);
            }
        }

        MPI_D.Finalize();
    }

    private static final Pattern SLASH = Pattern.compile("/");

    private static String getOutputName(int partition) {
        return "part-" + NumberFormat.getInstance().format(partition);
    }

    private static long createLabelIndex(Path labPath) throws IOException {
        long labelSize = 0;
        Iterable<Pair<Text, IntWritable>> iterable = new SequenceFileDirIterable<Text, IntWritable>(
                inPath, PathType.LIST, PathFilters.logsCRCFilter(), config);
        labelSize = BayesUtils.writeLabelIndex(config, labPath, iterable);
        return labelSize;
    }

    private static void initConf(HashMap<String, String> conf) {
        conf.put(MPI_D_Constants.ReservedKeys.KEY_CLASS,
                IntWritable.class.getName());
        conf.put(MPI_D_Constants.ReservedKeys.VALUE_CLASS,
                VectorWritable.class.getName());
        conf.put(MPI_D_Constants.ReservedKeys.COMBINER_CLASS,
                IndexInstancesCombiner.class.getName());

        if (maxUsedMemPercent != null) {
            conf.put(
                    MPI_D_Constants.ReservedKeys.CommonModeKeys.MAX_MEM_USED_PERCENT,
                    maxUsedMemPercent);
        }
        if (partSize != null) {
            conf.put(MPI_D_Constants.ReservedKeys.CommonModeKeys.BLOCK_SIZE,
                    partSize);
        }
        if (outFileNum != null) {
            conf.put(
                    MPI_D_Constants.ReservedKeys.CommonModeKeys.SEND_QUEUE_LENGTH,
                    outFileNum);
        }
        if (spillPercent != null) {
            conf.put(MPI_D_Constants.ReservedKeys.CommonModeKeys.SPILL_PERCENT,
                    spillPercent);
        }
        config = new JobConf(confPath);
    }

    private static void parseArgs(String[] args) {
        if (args.length < 4) {
            System.err.println("ERROR: Wrong number of parameters: "
                    + args.length + " instead of 4.");
            System.err
                    .println("Usage: IndexInstances <confPath> <inDir> <outDir> <label>");
            System.exit(-1);
        } else if (args.length == 4) {
            confPath = args[0];
            inDir = args[1];
            outDir = args[2];
            labDir = args[3];
            outDir += "/" + BayesUtils.SUMMED_OBSERVATIONS;
            inPath = new Path(inDir);
            labPath = new Path(labDir);
        } else if (args.length <= 8) {
            // Advanced Usage
            confPath = args[0];
            inDir = args[1];
            outDir = args[2];
            labDir = args[3];
            maxUsedMemPercent = args[4];
            partSize = args[5];
            outFileNum = args[6];
            spillPercent = args[7];
            outDir += "/" + BayesUtils.SUMMED_OBSERVATIONS;
            inPath = new Path(inDir);
            labPath = new Path(labDir);
        } else {
            System.err.println("ERROR: Error number of parameters.");
            System.exit(-1);
        }

    }
}
