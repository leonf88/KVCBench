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

package mlbench.kmeans;

import mpi.MPIException;
import mpid.core.MPI_D;
import mpid.core.MPI_D_Exception;
import mpid.core.util.MPI_D_Constants;
import mpid.util.DataMPIUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.mahout.math.VectorWritable;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;


public class KmeansInit {
    private static JobConf config = null;
    private static String confPath = null;
    private static List<PointVector> centers = new ArrayList<>();
    private static int rank = -1;
    private static int size = -1;
    private static int kCluster = 0;
    private static String dataPath = null, outPath = null,
            maxUsedMemPercent = null, partSize = null, outFileNum = null,
            spillPercent = null;
    private static KmeansUtils.CenterTransfer transfer = null;

    public static void main(String[] args) throws MPI_D_Exception, IOException, MPIException {
        parseArgs(args);
        HashMap<String, String> conf = new HashMap<String, String>();
        initConf(conf);

        init(args, dataPath, kCluster, conf);
    }

    private static void initConf(HashMap<String, String> conf) {
        conf.put(MPI_D_Constants.ReservedKeys.KEY_CLASS, Text.class.getName());
        conf.put(MPI_D_Constants.ReservedKeys.VALUE_CLASS, PointVector.class.getName());

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
        config = new JobConf(confPath);
    }

    private static void parseArgs(String[] args) {
        if (args.length < 4) {
            System.err.printf("ERROR: Wrong number of parameters: %d instead of 4.\n", args.length);
            System.err.println("Usage: KmeansInit <HDFS config path> <input path> <output path> " +
                    "<number of clusters>");
            System.exit(-1);
        } else if (args.length == 4) {
            confPath = args[0];
            dataPath = args[1];
            outPath = args[2];
            kCluster = Integer.valueOf(args[3]);
        } else if (args.length <= 8) {
            // Advanced Usage
            confPath = args[0];
            dataPath = args[1];
            outPath = args[2];
            kCluster = Integer.valueOf(args[3]);
            maxUsedMemPercent = args[4];
            partSize = args[5];
            outFileNum = args[6];
            spillPercent = args[7];
        } else {
            System.err.println("ERROR: Error number of parameters.");
            System.exit(-1);
        }
    }

    /**
     * get the input values and choose the K clusters' centers
     *
     * @param dataPath
     * @throws MPI_D_Exception
     * @throws IOException
     * @throws MPIException
     */
    @SuppressWarnings("deprecation")
    private static void init(String args[], String dataPath, int kCluster, HashMap<String,
            String> conf) throws MPI_D_Exception, IOException, MPIException {
        MPI_D.Init(args, MPI_D.Mode.Common, conf);
        if (MPI_D.COMM_BIPARTITE_O != null) {
            rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_O);
            size = MPI_D.Comm_size(MPI_D.COMM_BIPARTITE_O);
            FileSplit[] inputs = DataMPIUtil.HDFSDataLocalLocator.getTaskInputs(
                    MPI_D.COMM_BIPARTITE_O, config, dataPath, rank);


            // for record the initialized state
            for (FileSplit path : inputs) {
                SequenceFileInputFormat f = new SequenceFileInputFormat();
                JobConf jobConf = new JobConf(confPath);
                Reporter r = new KmeansUtils.EmptyReport();
                RecordReader<LongWritable, VectorWritable> reader = f.getRecordReader(path,
                        jobConf, r);

                Random random = new Random(1000);
                LongWritable k = reader.createKey();
                VectorWritable v = reader.createValue();

                while (reader.next(k, v)) {
                    PointVector p = new PointVector(random.nextInt(kCluster), v);
                    MPI_D.Send(new Text(Long.toString(p.getStrClusterClass())), p);
                }
                reader.close();
//                DataInputStream in = KmeansUtils.readFromHDFSF(path.getPath(), config);
//
//                String lineVal;
//                Random random = new Random(1000);
//                try {
//                    while ((lineVal = in.readLine()) != null) {
//                        double[] var = format(lineVal);
//                        if (var != null) {
//                            PointVector p = new PointVector(random.nextInt(kCluster), var);
//                            MPI_D.Send(new Text(Long.toString(p.getStrClusterClass())), p);
//                        }
//                    }
//                } catch (IOException | MPI_D_Exception e) {
//                    e.printStackTrace();
//                } finally {
//                    try {
//                        in.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
            }
        } else {
            Text key = null, newKey = null;
            PointVector point = null, newPoint = null;
            double sum[] = null;
            int count = 0;
            Object[] vals = MPI_D.Recv();
            while (vals != null) {
                newKey = (Text) vals[0];
                newPoint = (PointVector) vals[1];
                if (key == null && point == null) {
                    sum = new double[newPoint.getValueD().length];
                } else if (!key.equals(newKey)) {
                    double[] centerVals = new double[sum.length];
                    for (int i = 0; i < centerVals.length; i++) {
                        centerVals[i] = (double) sum[i] / count;
                    }
                    PointVector oneCenter = new PointVector(Long.valueOf(key
                            .toString()), centerVals);
                    centers.add(oneCenter);
                    sum = new double[point.getValueD().length];
                    count = 0;
                }
                key = newKey;
                point = newPoint;
                KmeansUtils.accumulate(sum, newPoint.getDoubleValue());
                count++;
                vals = MPI_D.Recv();
            }
            if (newKey != null && newPoint != null) {
                double[] centerVals = new double[sum.length];
                for (int i = 0; i < centerVals.length; i++) {
                    centerVals[i] = (double) sum[i] / count;
                }
                PointVector oneCenter = new PointVector(Long.valueOf(key
                        .toString()), centerVals);
                centers.add(oneCenter);
            }

            transfer = new KmeansUtils.CenterTransfer(config, rank, size);
            transfer.gatherCentersByP2P(centers);

            {
                if (rank == 0) {
                    OutputStream resOut = KmeansUtils.getOutputStream(outPath, config);
                    DataOutput os = new DataOutputStream(resOut);

                    for (PointVector centerPoint : centers) {
                        os.write((centerPoint.toString() + "\n").getBytes());
                    }
                    resOut.flush();
                    resOut.close();
                }
            }

            System.out.println("rank " + rank + " finish");
        }
        MPI_D.Finalize();
    }

    private static double[] format(String line) {
        String vals[] = line.split("\\s+");
        double fvals[] = new double[vals.length];
        try {
            for (int i = 0; i < fvals.length; i++) {
                fvals[i] = Double.valueOf(vals[i]);
            }
        } catch (Exception e) {
            return null;
        }

        return fvals;
    }
}
