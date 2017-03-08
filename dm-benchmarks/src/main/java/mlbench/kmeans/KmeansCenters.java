package mlbench.kmeans;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KmeansCenters implements Writable {
    private long pointSize;
    private double[] vector;

    public KmeansCenters() {
        pointSize = -1;
        vector = null;
    }

    public KmeansCenters(long size, double[] vector) {
        this.pointSize = size;
        this.vector = vector;
    }

    public double[] getVector() {
        return vector;
    }

    public long getPointSize() {
        return pointSize;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pointSize = in.readLong();
        int len = in.readInt();
        vector = new double[len];
        for (int i = 0; i < len; i++) {
            vector[i] = in.readDouble();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(pointSize);
        out.writeInt(vector.length);
        for (int i = 0; i < vector.length; i++) {
            out.writeDouble(vector[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(pointSize).append(":");
        for (double val : vector) {
            sb.append(val).append(" ");
        }
        return sb.toString();
    }
}
