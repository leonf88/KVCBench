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
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


public class WeightSummer {

    private static Configuration config;
    private static final String MAPRED_OUTPUT_DIR = "mapred.output.dir";
    private static String confPath = null;
    private static String inDir = null, outDir = null, outDirW = null,
            maxUsedMemPercent = null, partSize = null, outFileNum = null,
            spillPercent = null;
    private static int labNum;

    public static final String WEIGHTS_PER_FEATURE = "__SPF";
    public static final String WEIGHTS_PER_LABEL = "__SPL";
    public static final String LABEL_THETA_NORMALIZER = "_LTN";

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

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws MPI_D_Exception, IOException,
            MPIException {
        parseArgs(args);
        HashMap<String, String> conf = new HashMap<String, String>();
        initConf(conf);
        MPI_D.Init(args, MPI_D.Mode.Common, conf);
        if (MPI_D.COMM_BIPARTITE_O != null) {

            int rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_O);
            int size = MPI_D.Comm_size(MPI_D.COMM_BIPARTITE_O);
            FileSplit[] inputs = DataMPIUtil.HDFSDataLocalLocator.getTaskInputs(
                    MPI_D.COMM_BIPARTITE_O, (JobConf) config, inDir, rank);
            Vector weightsPerFeature = null;
            Vector weightsPerLabel = new DenseVector(labNum);

            for (int i = 0; i < inputs.length; i++) {
                FileSplit fsplit = inputs[i];
                SequenceFileRecordReader<IntWritable, VectorWritable> kvrr = new
                        SequenceFileRecordReader<>(
                        config, fsplit);
                IntWritable index = kvrr.createKey();
                VectorWritable value = kvrr.createValue();
                while (kvrr.next(index, value)) {
                    Vector instance = value.get();
                    if (weightsPerFeature == null) {
                        weightsPerFeature = new RandomAccessSparseVector(
                                instance.size(),
                                instance.getNumNondefaultElements());
                    }

                    int label = index.get();
                    weightsPerFeature.assign(instance, Functions.PLUS);
                    weightsPerLabel.set(label, weightsPerLabel.get(label)
                            + instance.zSum());
                }
            }
            if (weightsPerFeature != null) {
                MPI_D.Send(new Text(WEIGHTS_PER_FEATURE), new VectorWritable(
                        weightsPerFeature));
                MPI_D.Send(new Text(WEIGHTS_PER_LABEL), new VectorWritable(
                        weightsPerLabel));
            }
        } else if (MPI_D.COMM_BIPARTITE_A != null) {
            int rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_A);
            config.set(MAPRED_OUTPUT_DIR, outDirW);
            config.set("mapred.task.id", DataMPIUtil.getHadoopTaskAttemptID()
                    .toString().toString());
            ((JobConf) config).setOutputKeyClass(Text.class);
            ((JobConf) config).setOutputValueClass(VectorWritable.class);
            TaskAttemptContext taskContext = new TaskAttemptContextImpl(config,
                    DataMPIUtil.getHadoopTaskAttemptID());
            SequenceFileOutputFormat<Text, VectorWritable> outfile = new
                    SequenceFileOutputFormat<>();
            FileSystem fs = FileSystem.get(config);

            Path output = new Path(config.get(MAPRED_OUTPUT_DIR));
            FileOutputCommitter fcommitter = new FileOutputCommitter(output,
                    taskContext);
            RecordWriter<Text, VectorWritable> outrw = null;
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

            Text key = null, newKey = null;
            VectorWritable point = null, newPoint = null;
            Vector vector = null;
            Object[] vals = MPI_D.Recv();
            while (vals != null) {
                newKey = (Text) vals[0];
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

            MPI_D.COMM_BIPARTITE_A.Barrier();
            if (rank == 0) {
                Path resOut = new Path(outDir);
                NaiveBayesModel naiveBayesModel = BayesUtils.readModelFromDir(
                        new Path(outDir), config);
                naiveBayesModel.serialize(resOut, config);
            }
        }

        MPI_D.Finalize();
    }

    private static String getOutputName(int partition) {
        return "part-" + NumberFormat.getInstance().format(partition);
    }

    private static void initConf(HashMap<String, String> conf) {
        conf.put(MPI_D_Constants.ReservedKeys.KEY_CLASS, Text.class.getName());
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
                    .println("Usage: WeightSummer <confPath> <inDir> <outDir> <label number>");
            System.exit(-1);
        } else if (args.length == 4) {
            confPath = args[0];
            inDir = args[1];
            outDir = args[2];
            labNum = Integer.valueOf(args[3]);
            outDirW = outDir + "/" + BayesUtils.WEIGHTS;
        } else if (args.length <= 8) {
            // Advanced Usage
            confPath = args[0];
            inDir = args[1];
            outDir = args[2];
            labNum = Integer.valueOf(args[3]);
            maxUsedMemPercent = args[4];
            partSize = args[5];
            outFileNum = args[6];
            spillPercent = args[7];
            outDirW = outDir + "/" + BayesUtils.WEIGHTS;
        } else {
            System.err.println("ERROR: Error number of parameters.");
            System.exit(-1);
        }

    }

}
