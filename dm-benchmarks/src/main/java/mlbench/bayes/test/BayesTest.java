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

package mlbench.bayes.test;

import mlbench.bayes.BayesUtils;
import mpi.MPIException;
import mpid.core.MPI_D;
import mpid.core.MPI_D_Exception;
import mpid.core.util.MPI_D_Constants;
import mpid.util.DataMPIUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.mahout.classifier.ClassifierResult;
import org.apache.mahout.classifier.ResultAnalyzer;
import org.apache.mahout.classifier.naivebayes.AbstractNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;


public class BayesTest {

    private static Configuration config;
    private static final String MAPRED_OUTPUT_DIR = "mapred.output.dir";
    private static String confPath = null;
    private static String inDir = null, outDir = null, labDir = null,
            modelDir = null, maxUsedMemPercent = null, partSize = null,
            outFileNum = null, spillPercent = null;
    private static Path labPath = null, modelPath;
    private static int rank = -1;
    private static int size = -1;

    private static final Pattern SLASH = Pattern.compile("/");
    private static final String TESTING_RESULTS = "testing_result";
    private static AbstractNaiveBayesClassifier classifier;

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws MPI_D_Exception, IOException,
            MPIException {
        parseArgs(args);
        HashMap<String, String> conf = new HashMap<String, String>();
        initConf(conf);
        MPI_D.Init(args, MPI_D.Mode.Common, conf);

        if (MPI_D.COMM_BIPARTITE_O != null) {
            rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_O);
            size = MPI_D.Comm_size(MPI_D.COMM_BIPARTITE_O);
            NaiveBayesModel model = NaiveBayesModel.materialize(modelPath,
                    config);
            classifier = new StandardNaiveBayesClassifier(model);

            MPI_D.COMM_BIPARTITE_O.Barrier();
            FileSplit[] inputs = DataMPIUtil.HDFSDataLocalLocator
                    .getTaskInputs(MPI_D.COMM_BIPARTITE_O, (JobConf) config,
                            inDir, rank);

            for (int i = 0; i < inputs.length; i++) {
                FileSplit fsplit = inputs[i];
                SequenceFileRecordReader<Text, VectorWritable> kvrr = new
                        SequenceFileRecordReader<>(
                        config, fsplit);
                Text key = kvrr.createKey();
                VectorWritable value = kvrr.createValue();
                while (kvrr.next(key, value)) {
                    Vector result = classifier.classifyFull(value.get());
                    MPI_D.Send(new Text(SLASH.split(key.toString())[1]),
                            new VectorWritable(result));
                }
            }
        } else if (MPI_D.COMM_BIPARTITE_A != null) {
            int rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_A);
            config.set(MAPRED_OUTPUT_DIR, outDir);
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
            Text key = null;
            VectorWritable point = null;
            Vector vector = null;
            Object[] vals = MPI_D.Recv();
            while (vals != null) {
                key = (Text) vals[0];
                point = (VectorWritable) vals[1];
                if (key != null && point != null) {
                    vector = point.get();
                    outrw.write(key, new VectorWritable(vector));
                }
                vals = MPI_D.Recv();
            }
            outrw.close(null);
            if (fcommitter.needsTaskCommit(taskContext)) {
                fcommitter.commitTask(taskContext);
            }

            MPI_D.COMM_BIPARTITE_A.Barrier();
            if (rank == 0) {
                // load the labels
                Map<Integer, String> labelMap = BayesUtils.readLabelIndex(
                        config, labPath);
                // loop over the results and create the confusion matrix
                SequenceFileDirIterable<Text, VectorWritable> dirIterable = new
                        SequenceFileDirIterable<Text, VectorWritable>(
                        output, PathType.LIST, PathFilters.partFilter(), config);
                ResultAnalyzer analyzer = new ResultAnalyzer(labelMap.values(),
                        "DEFAULT");
                analyzeResults(labelMap, dirIterable, analyzer);
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
        if (args.length < 5) {
            System.err.println("ERROR: Wrong number of parameters: "
                    + args.length + " instead of 5.");
            System.err
                    .println("Usage: BayesTest <confPath> <inDir> <outDir> <modelDir> <label>");
            System.exit(-1);
        } else if (args.length == 5) {
            confPath = args[0];
            inDir = args[1];
            modelDir = args[2];
            labDir = args[3];
            outDir = args[4];
            outDir = outDir + "/" + TESTING_RESULTS;
            labPath = new Path(labDir);
            modelPath = new Path(modelDir);
        } else if (args.length <= 9) {
            // Advanced Usage
            confPath = args[0];
            inDir = args[1];
            modelDir = args[2];
            labDir = args[3];
            outDir = args[4];
            maxUsedMemPercent = args[5];
            partSize = args[6];
            outFileNum = args[7];
            spillPercent = args[8];
            outDir = outDir + "/" + TESTING_RESULTS;
            labPath = new Path(labDir);
            modelPath = new Path(modelDir);
        } else {
            System.err.println("ERROR: Error number of parameters.");
            System.exit(-1);
        }
    }

    private static void analyzeResults(Map<Integer, String> labelMap,
                                       SequenceFileDirIterable<Text, VectorWritable> dirIterable,
                                       ResultAnalyzer analyzer) {
        for (Pair<Text, VectorWritable> pair : dirIterable) {
            int bestIdx = Integer.MIN_VALUE;
            double bestScore = Long.MIN_VALUE;
            for (Vector.Element element : pair.getSecond().get().all()) {
                if (element.get() > bestScore) {
                    bestScore = element.get();
                    bestIdx = element.index();
                }
            }
            if (bestIdx != Integer.MIN_VALUE) {
                ClassifierResult classifierResult = new ClassifierResult(
                        labelMap.get(bestIdx), bestScore);
                analyzer.addInstance(pair.getFirst().toString(),
                        classifierResult);
            }
        }
    }
}
