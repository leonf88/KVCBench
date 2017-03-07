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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class KmeansIter {
    private static JobConf config = null;
    private static String confPath = null;
    private static List<PointVector> centers = new ArrayList<PointVector>();
    private static List<Vector> centersV = new ArrayList<>();
    private static int rank = -1;
    private static int size = -1;
    private static int kCluster = 0;
    private static String centerPath = null, dataPath = null, outPath = null,
            maxUsedMemPercent = null, partSize = null, outFileNum = null,
            spillPercent = null;

    public static void main(String[] args) throws MPI_D_Exception,
            MPIException, IOException {
        parseArgs(args);
        HashMap<String, String> conf = new HashMap<String, String>();
        initConf(conf);

        iterBody(args, conf);
    }

    private static void initConf(HashMap<String, String> conf) {
        conf.put(MPI_D_Constants.ReservedKeys.KEY_CLASS, Text.class.getName());
        conf.put(MPI_D_Constants.ReservedKeys.VALUE_CLASS,
                PointVector.class.getName());

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
                    .println("Usage: KmeansIter <HDFS config path> <data path> <cluster path> " +
                            "<output path> <number of centers>");
            System.exit(-1);
        } else if (args.length == 5) {
            confPath = args[0];
            dataPath = args[1];
            centerPath = args[2];
            outPath = args[3];
            kCluster = Integer.valueOf(args[4]);
        } else if (args.length <= 9) {
            // Advanced Usage
            confPath = args[0];
            dataPath = args[1];
            centerPath = args[2];
            outPath = args[3];
            kCluster = Integer.valueOf(args[4]);
            maxUsedMemPercent = args[5];
            partSize = args[6];
            outFileNum = args[7];
            spillPercent = args[8];
        } else {
            System.err.println("ERROR: Error number of parameters.");
            System.exit(-1);
        }
    }

    public static DataInputStream readFromHDFSF(Path path) {
        InputStream in = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            in = fs.open(path);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return new DataInputStream(in);
    }

    private static double[] format(String line) {
        String vals[] = line.split("\\s+");
        double fvals[] = new double[vals.length];
        for (int i = 0; i < fvals.length; i++) {
            fvals[i] = Double.valueOf(vals[i]);
        }
        return fvals;
    }

    private static PointVector getBelongPoint(VectorWritable v) throws MPI_D_Exception {
        long belong = -1;
        double min = Double.MAX_VALUE, val = -1;
        for (PointVector centerPoint : centers) {
            val = centerPoint.getVector().getDistanceSquared(v.get());
            if (val < min) {
                belong = centerPoint.getStrClusterClass();
                min = val;
            }
        }
        return new PointVector(belong, v);
    }

    private static PointVector getBelongPoint(String lineVal)
            throws MPI_D_Exception {
        double[] point = format(lineVal);
        long belong = -1;
        double min = Double.MAX_VALUE, val = -1;
        for (PointVector centerPoint : centers) {
            val = KmeansUtils.distance(centerPoint.getDoubleValue(), point);
            if (val < min) {
                belong = centerPoint.getStrClusterClass();
                min = val;
            }
        }
        return new PointVector(belong, point);
    }

    /**
     * Calculate the new center iteratively
     *
     * @return true: finish; false: continue
     * @throws MPI_D_Exception
     * @throws MPIException
     * @throws IOException
     */
    @SuppressWarnings("deprecation")
    private static void iterBody(String args[], HashMap<String, String> conf)
            throws MPI_D_Exception, MPIException, IOException {
        MPI_D.Init(args, MPI_D.Mode.Common, conf);

        if (MPI_D.COMM_BIPARTITE_O != null) {
            rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_O);
            size = MPI_D.Comm_size(MPI_D.COMM_BIPARTITE_O);

            if (rank == 0) {
                System.out.println(centerPath);
                DataInputStream in = KmeansUtils.readFromHDFSF(new Path(
                        centerPath), config);

                String lineVal;
                try {
                    while ((lineVal = in.readLine()) != null) {
                        String lineSeq[] = lineVal.split(":");
                        PointVector p = new PointVector(
                                Integer.valueOf(lineSeq[0]), format(lineSeq[1]));
                        centers.add(p);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        in.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            MPI_D.COMM_BIPARTITE_O.Barrier();

            KmeansUtils.CenterTransfer transfer = new KmeansUtils.CenterTransfer(config, rank,
                    size);
            transfer.broadcastCenters(centers);

            FileSplit[] inputs = DataMPIUtil.HDFSDataLocalLocator.getTaskInputs(
                    MPI_D.COMM_BIPARTITE_O, config, dataPath, rank);
            double centerSum[][] = new double[kCluster][];
            long centerPNum[] = new long[kCluster];

            // for record the initialized state
            for (FileSplit path : inputs) {
                SequenceFileInputFormat f = new SequenceFileInputFormat();
                JobConf jobConf = new JobConf(confPath);
                Reporter r = new KmeansUtils.EmptyReport();
                RecordReader<LongWritable, VectorWritable> reader = f.getRecordReader(
                        path, jobConf, r);
                LongWritable k = reader.createKey();
                VectorWritable v = reader.createValue();

                while (reader.next(k, v)) {
                    PointVector p = getBelongPoint(v);
                    int i = (int) p.getStrClusterClass();
                    double[] vals = p.getDoubleValue();
                    int len = vals.length;
                    if (centerSum[i] == null) {
                        centerSum[i] = new double[len];
                    }
                    for (int j = 0; j < len; j++) {
                        centerSum[i][j] += vals[j];
                    }
                    centerPNum[i]++;
                }
                reader.close();
            }
            for (int i = 0; i < centerPNum.length; i++) {
                if (centerSum[i] == null && centerPNum[i] == 0) {
                    continue;
                }
                MPI_D.Send(new Text(Integer.toString(i)), new PointVector(
                        centerPNum[i], centerSum[i]));
            }
        } else {
            centers.clear();
            Text key = null, newKey = null;
            PointVector point = null, newPoint = null;
            double sum[] = null;
            long count = 0;
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
                    PointVector oneCenter = new PointVector(
                            Long.valueOf(key.toString()), centerVals);
                    centers.add(oneCenter);
                    sum = new double[point.getValueD().length];
                    count = 0;
                }
                key = newKey;
                point = newPoint;
                KmeansUtils.accumulate(sum, newPoint.getDoubleValue());
                count += Long.valueOf(newPoint.getStrClusterClass());
                vals = MPI_D.Recv();
            }
            if (newKey != null && newPoint != null) {
                double[] centerVals = new double[sum.length];
                for (int i = 0; i < centerVals.length; i++) {
                    centerVals[i] = (double) sum[i] / count;
                }
                PointVector oneCenter = new PointVector(Long.valueOf(key.toString()), centerVals);
                centers.add(oneCenter);
            }

            KmeansUtils.CenterTransfer transfer =
                    new KmeansUtils.CenterTransfer(config, rank, size);
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
        }
        MPI_D.Finalize();
    }
}
