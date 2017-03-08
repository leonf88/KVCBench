package mlbench.kmeans;

import mpid.core.MPI_D;
import mpid.core.MPI_D_Context;
import mpid.core.MPI_D_Exception;
import mpid.core.MPI_D_Partitioner;
import mpid.core.util.MPI_D_Constants;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Kmeans {

    private static PointWriable[] getCenters(Path centerPath, int centerNum) throws IOException {
        JobConf conf = new JobConf();
        FileSystem fs = centerPath.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(centerPath);
        FileSplit fsplit = new FileSplit(status.getPath(), 0, status.getLen(), conf);
        LineRecordReader reader = new LineRecordReader(conf, fsplit);
        LongWritable key = reader.createKey();
        Text value = reader.createValue();

        PointWriable centers[] = new PointWriable[centerNum];
        for (int i = 0; i < centers.length; i++) {
            reader.next(key, value);
            centers[i] = new PointWriable(value);
        }

        return centers;
    }

    private static class KMeansPartitionLocal implements MPI_D_Partitioner {

        int taskRank;

        @Override
        public int getPartition(Object key, Object value, int numPartitions) throws
                MPI_D_Exception {
            return taskRank;
        }

        @Override
        public void configure(MPI_D_Context context) throws MPI_D_Exception {
            taskRank = context.threadRank;
        }

    }

    private static int size, rank;

    public static void run(String[] args, int taskRank) throws MPI_D_Exception, IOException {
        HashMap<String, String> conf = new HashMap<String, String>();
        conf.put(MPI_D_Constants.ReservedKeys.CommonModeKeys.BLOCK_METADATA_PERCENT, "0.4");
        conf.put("mpid.sort.enable", "false");
        conf.put(MPI_D_Constants.ReservedKeys.COMPARATOR_CLASS,
                PointWritableComparator.class.getName());
        conf.put(MPI_D_Constants.ReservedKeys.PARTITIONER_CLASS,
                KMeansPartitionLocal.class.getName());
        MPI_D.Init(args, MPI_D.Mode.Iteration, conf, taskRank);

        size = MPI_D.Comm_size(taskRank, MPI_D.COMM_BIPARTITE_O);
        rank = MPI_D.Comm_rank(taskRank, MPI_D.COMM_BIPARTITE_O);
        // seri = Serialization4Centers.getSinglon(new JobConf(), size, rank);

        PointWriable centers[] = getCenters(new Path(args[1]), Integer.valueOf(args[2]));
        // System.out.println(Arrays.toString(centers));
        int dimension = Integer.valueOf(args[3]);
        double BIAS = Double.valueOf(args[4]);
        int iterCount = Integer.valueOf(args[4]);

        MPI_D.setKVClass(taskRank, PointWriable.class, NullWritable.class, Text.class,
                NullWritable.class);
        MPI_D.setSource(taskRank, new Path(args[0])); // set input
        int recvHandler = MPI_D.makeOneBufferRegion(taskRank);

        List<Long> times = new ArrayList<>();
        long t1 = System.currentTimeMillis();
        // cache the keys
        Object[] kv = MPI_D.Recv(taskRank);
        while (kv != null) {
            MPI_D.Send(taskRank, new PointWriable((Text) kv[0]), NullWritable.get());
            kv = MPI_D.Recv(taskRank);
        }

        times.add(System.currentTimeMillis() - t1);
        System.out.println("app over");
        MPI_D.stopStage(taskRank);

        // debug(recvHandler, taskRank);

        double bias2 = 10;
        for (int i = 0; i < iterCount; i++) {
            t1 = System.currentTimeMillis();
            PointWriable c2[] = oneIteration(recvHandler, centers, dimension, taskRank);
            // if (rank == 0) {
            // System.out.println(Arrays.toString(c2));
            // }
            bias2 = getBaisBetweenCenters(centers, c2);
            centers = c2;
            times.add(System.currentTimeMillis() - t1);
            if (rank == 0 && taskRank == 0)
                System.out.println("BIAS: " + bias2);
        }
        // } while (bias2 > BIAS);

        if (rank == 0 && taskRank == 0) {
            printCenter(centers);

        }
        for (int i = 0; i < times.size(); i++) {
            System.out.println(rank + " " + taskRank + " Iteration " + i + " cost " + times.get(i)
                    + " ms.");
        }

        MPI_D.Finalize(taskRank);
    }

    static void printCenter(PointWriable centers[]) {
        for (int i = 0; i < centers.length; i++) {
            System.out.println(String.format("Center %d, Vectors: %s", i, centers[i]));
        }
    }

    static double getBaisBetweenCenters(PointWriable center1[], PointWriable center2[]) {
        if (center1.length != center2.length) {
            throw new IllegalArgumentException("Center sizes don't match");
        }
        int bais = 0;
        for (int i = 0; i < center1.length; i++) {
            bais += calucateDistence(center1[i], center2[i]);
        }

        return bais;
    }

    private static double calucateDistence(PointWriable p1, PointWriable p2) {
        return calucateDistence(p1.getValues(), p2.getValues());
    }

    private static double calucateDistence(double p1[], double p2[]) {
        if (p1.length != p2.length) {
            throw new IllegalArgumentException("Vector sizes don't match");
        }
        double res = 0;
        for (int i = 0; i < p2.length; i++) {
            res += (p1[i] - p2[i]) * (p1[i] - p2[i]);
        }
        return res;
    }

    private static int findCloset(PointWriable point, PointWriable centers[]) {
        double bestDistance = Double.MAX_VALUE;
        int centerIdx = -1;
        for (int i = 0; i < centers.length; i++) {
            double d = calucateDistence(point.getValues(), centers[i].getValues());
            if (d < bestDistance) {
                bestDistance = d;
                centerIdx = i;
            }
        }

        return centerIdx;
    }

    // static Serialization4Centers seri;

    static void debug(int handler, int taskRank) throws MPI_D_Exception, IOException {
        System.out.println("create " + handler);
        MPI_D.setKVClass(taskRank, PointWriable.class, NullWritable.class, PointWriable.class,
                NullWritable.class);
        MPI_D.setSource(taskRank, handler); // set input

        int count = 0;
        Object kv[] = MPI_D.Recv(taskRank);
        while (kv != null) {
            System.out.println((PointWriable) kv[0]);
            count++;
            kv = MPI_D.Recv(taskRank);
        }

        System.out.println("output " + count);

        MPI_D.stopStage(taskRank);
    }

    List<Long> timelList = new ArrayList<>();

    private static PointWriable[] oneIteration(int handler, PointWriable cp[], int dimension,
                                               int taskRank) throws MPI_D_Exception, IOException {
        long t1 = System.currentTimeMillis();
        MPI_D.setKVClass(taskRank, PointWriable.class, NullWritable.class, PointWriable.class,
                NullWritable.class);

        MPI_D.setSource(taskRank, handler); // set input

        PointWriable newCenters[] = new PointWriable[cp.length];
        for (int i = 0; i < newCenters.length; i++) {
            newCenters[i] = new PointWriable(dimension);
        }

        int centerCount[] = new int[cp.length];
        Object[] kv = MPI_D.Recv(taskRank);
        while (kv != null) {
            // System.out.println("read " + kv[0]);
            int c = findCloset(((PointWriable) kv[0]), cp);
            newCenters[c].add((PointWriable) kv[0]);
            centerCount[c]++;
            kv = MPI_D.Recv(taskRank);
        }
        MPI_D.stopStage(taskRank);
        long t2 = System.currentTimeMillis();
        System.out.println(rank + " read " + taskRank + " cost " + (t2 - t1) + " ms.");

        // for (int i = 0; i < newCenters.length; i++) {
        // newCenters[i].divide(centerCount[i]);
        // }
        // System.out.println("before " + Arrays.toString(newCenters));

        MPI_D.setKVClass(taskRank, IntWritable.class, Center.class, PointWriable.class,
                NullWritable.class);
        IntWritable intw = new IntWritable();
        int recvHandler = MPI_D.makeOneBufferRegion(taskRank);
        for (int i = 0; i < newCenters.length; i++) {
            Center center = new Center(i, centerCount[i], newCenters[i].values);
            // System.out.println("send all " + center);
            intw.set(i);
            MPI_D.SendAll(taskRank, intw, center);
        }
        MPI_D.stopStage(taskRank);

        MPI_D.setKVClass(taskRank, IntWritable.class, Center.class, IntWritable.class, Center
                .class);
        MPI_D.setSource(taskRank, recvHandler); // set input

        Center centers[] = new Center[cp.length];
        kv = MPI_D.Recv(taskRank);
        while (kv != null) {
            Center c = (Center) kv[1];
            // System.out.println(rank + " " + taskRank + " recv " + c);
            if (centers[c.centerCls] == null) {
                centers[c.centerCls] = new Center(c.centerCls, c.count, c.values.clone());
            } else {
                centers[c.centerCls].add(c);
            }
            kv = MPI_D.Recv(taskRank);
        }
        for (int i = 0; i < centers.length; i++) {
            centers[i].divideByCount();
            newCenters[i].values = centers[i].values;
        }
        MPI_D.stopStage(taskRank);
        MPI_D.releaseRecvBuffer(taskRank, recvHandler);

        // System.out.println("after " + Arrays.toString(newCenters));
        t1 = System.currentTimeMillis();
        System.out.println(rank + " read " + taskRank + " broadcast " + (t1 - t2) + " ms.");
        return newCenters;
    }

    private static class Center implements Writable {
        int centerCls;
        int count;
        double values[];

        Center() {
        }

        public void add(Center c) {
            count += c.count;
            for (int i = 0; i < values.length; i++) {
                values[i] += c.values[i];
            }
        }

        public void divideByCount() {
            for (int i = 0; i < values.length; i++) {
                values[i] /= count;
            }
        }

        Center(int centerCls, int count, double values[]) {
            this.centerCls = centerCls;
            this.count = count;
            this.values = values;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(centerCls);
            out.writeInt(count);
            out.writeInt(values.length);
            for (double v : values) {
                out.writeDouble(v);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            centerCls = in.readInt();
            count = in.readInt();
            int w = in.readInt();
            values = new double[w];
            for (int i = 0; i < values.length; i++) {
                values[i] = in.readDouble();
            }
        }

        @Override
        public String toString() {
            return centerCls + " " + count + " " + Arrays.toString(values);
        }
    }
}
