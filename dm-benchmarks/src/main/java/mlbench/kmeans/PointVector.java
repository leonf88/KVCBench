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

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PointVector implements Writable {
    private int cluster;
    private double[] vector;
    private Vector v;

    public PointVector() {
        cluster = -1;
        vector = null;
    }

    public PointVector(int cluster, VectorWritable vector) {
        super();
        this.cluster = cluster;
        int nsize = vector.get().size();
        this.vector = new double[nsize];
        for (int i = 0; i < nsize; i++) {
            this.vector[i] = vector.get().get(i);
        }
    }

    public Vector getDenseVector() {
        if (v == null) {
            v = new DenseVector(vector);
        }
        return v;
    }

    public PointVector(int cluster, double[] value) {
        super();
        this.cluster = cluster;
        vector = value;
    }

    public double[] getDoubleValue() {
        return vector;
    }

    public void setVector(double[] value) {
        this.vector = value;
    }

    public void setVector(VectorWritable vector) {

    }

    public int getCluster() {
        return cluster;
    }

    public void setCluster(int cluster) {
        this.cluster = cluster;
    }

    public double[] getVector() {
        return vector;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        cluster = in.readInt();
        int len = in.readInt();
        vector = new double[len];
        for (int i = 0; i < len; i++) {
            vector[i] = in.readDouble();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(cluster);
        out.writeInt(vector.length);
        for (int i = 0; i < vector.length; i++) {
            out.writeDouble(vector[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(cluster).append(":");
        for (double val : vector) {
            sb.append(val).append(" ");
        }
        return sb.toString();
    }
}
