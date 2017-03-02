package org.apache.mahout.clustering.kmeans2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans2.KMeansDriver;
import org.apache.mahout.clustering.kmeans2.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class KMeansMain {
    private static final String DIRECTORY_CONTAINING_CONVERTED_INPUT = "data";

    public KMeansMain() {
    }

    private static void printCenters(Path pathToFile, Path centerFile) throws Exception {
        System.out.println("Show KMeans Cluster Centers");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, pathToFile, conf);
        Text key = new Text();
        ClusterWritable val = new ClusterWritable();
        FSDataOutputStream out = fs.create(centerFile);
        System.out.println(centerFile);

        while (reader.next(key, val)) {
            byte[] buff = (key + ":" + val.getValue().getCenter().toString() + "\n").getBytes();
            out.write(buff, 0, buff.length);
        }

        out.close();
    }

    private static void Kmeansrun(Configuration conf, Path input, Path output, DistanceMeasure
            measure, int k, double convergenceDelta, int maxIterations) throws Exception {
        Path directoryContainingConvertedInput = new Path(output, "seqdata");
        System.out.println("Preparing Input");
        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math" +
                ".RandomAccessSparseVector");
        System.out.println("Running random seed to get initial clusters");
        Path clusters = new Path(output, "clusters-0");
        clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput,
                clusters, k, measure);
        System.out.println("Running KMeans");
        KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, measure,
                convergenceDelta, maxIterations, true, 0.0D, false);
        printCenters(new Path(output, "clusters-*-final"), new Path(output, "clusteredPoints"));
    }

    public static void run1(Configuration conf, Path input, Path output, Path clusters,
                            DistanceMeasure measure, int k, double convergenceDelta, int
                                    maxIterations, int reduceNum) throws Exception {
        Path directoryContainingConvertedInput = new Path(output, "data");
        long t1 = System.currentTimeMillis();
        System.out.println("Running KMeans");
        KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, measure,
                convergenceDelta, maxIterations, true, 0.0D, false, reduceNum);
        t1 = System.currentTimeMillis();
        ClusterDumper clusterDumper = new ClusterDumper(new Path(output, "clusters-*-final"), new
                Path(output, "clusteredPoints"));
        System.out.println(clusterDumper.getTermDictionary());
        System.out.println("Output cost(ms): " + (System.currentTimeMillis() - t1));
    }

    public static void run(Configuration conf, Path input, Path output, Path clusters,
                           DistanceMeasure measure, int k, double convergenceDelta, int
                                   maxIterations, int reduceNum) throws Exception {
        Path directoryContainingConvertedInput = new Path(output, "data");
        System.out.println("Preparing Input");
        long t1 = System.currentTimeMillis();
        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math" +
                ".RandomAccessSparseVector");
        System.out.println("Preparing Input cost(ms): " + (System.currentTimeMillis() - t1));
        System.out.println("Running KMeans");
        KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, measure,
                convergenceDelta, maxIterations, false, 0.0D, false, reduceNum);
        System.out.println("Output cost( sec ): " + (System.currentTimeMillis() - t1) / 1000L);
    }

    public static void run(Configuration conf, Path input, Path output, DistanceMeasure measure,
                           int k, double convergenceDelta, int maxIterations, int reduceNum)
            throws Exception {
        Path directoryContainingConvertedInput = new Path(output, "data");
        System.out.println("Preparing Input");
        long t1 = System.currentTimeMillis();
        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math" +
                ".RandomAccessSparseVector");
        System.out.println("Preparing Input cost(ms): " + (System.currentTimeMillis() - t1));
        System.out.println("Running random seed to get initial clusters");
        t1 = System.currentTimeMillis();
        Path clusters = new Path(output, "clusters-0");
        clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput,
                clusters, k, measure);
        System.out.println("Random seed to get initial clusters cost(ms): " + (System
                .currentTimeMillis() - t1));
        System.out.println("Cluster path: " + clusters);
        System.out.println("Running KMeans");
        KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, measure,
                convergenceDelta, maxIterations, true, 0.0D, false, reduceNum);
        ClusterDumper clusterDumper = new ClusterDumper(new Path(output, "clusters-*-final"), new
                Path(output, "clusteredPoints"));
        System.out.println(clusterDumper.getTermDictionary());
        clusterDumper.printClusters((String[]) null);
    }

    public static void run(Configuration conf, Path input, Path output, DistanceMeasure measure,
                           int k, double convergenceDelta, int maxIterations) throws Exception {
        Path directoryContainingConvertedInput = new Path(output, "data");
        System.out.println("Preparing Input");
        long t1 = System.currentTimeMillis();
        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math" +
                ".RandomAccessSparseVector");
        System.out.println("Preparing Input cost(ms): " + (System.currentTimeMillis() - t1));
        System.out.println("Running random seed to get initial clusters");
        t1 = System.currentTimeMillis();
        Path clusters = new Path(output, "clusters-0");
        clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput,
                clusters, k, measure);
        System.out.println("Random seed to get initial clusters cost(ms): " + (System
                .currentTimeMillis() - t1));
        KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, measure,
                convergenceDelta, maxIterations, true, 0.0D, false);
        ClusterDumper clusterDumper = new ClusterDumper(new Path(output, "clusters-*-final"), new
                Path(output, "clusteredPoints"));
        System.out.println(clusterDumper.getTermDictionary());
        clusterDumper.printClusters((String[]) null);
    }

    public static void main(String[] args) {
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        System.out.println("Input Source: " + input);

        try {
            Configuration ex = new Configuration();
            HadoopUtil.delete(ex, new Path[]{output});
            int k = Integer.valueOf(args[2]).intValue();
            int maxIterations = Integer.valueOf(args[3]).intValue();
            double convergenceDelta = (double) Float.valueOf(args[4]).floatValue();
            if (args.length == 5) {
                run(ex, input, output, new EuclideanDistanceMeasure(), k, convergenceDelta,
                        maxIterations);
            }

            int redNum;
            if (args.length == 6) {
                redNum = Integer.valueOf(args[5]).intValue();
                run(ex, input, output, new EuclideanDistanceMeasure(), k, convergenceDelta,
                        maxIterations, redNum);
            }

            if (args.length == 7) {
                redNum = Integer.valueOf(args[5]).intValue();
                Path clusters = new Path(args[6]);
                run(ex, input, output, clusters, new EuclideanDistanceMeasure(), k,
                        convergenceDelta, maxIterations, redNum);
            }
        } catch (Exception var10) {
            var10.printStackTrace();
        }

    }
}

