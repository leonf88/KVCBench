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
    private long clusterClass;
    private double[] valueD;
    private Vector v;

    public PointVector() {
        clusterClass = -1;
        valueD = null;
    }

    public PointVector(long clusterClass, VectorWritable vector) {
        super();
        this.clusterClass = clusterClass;
        int nsize = vector.get().size();
        valueD = new double[nsize];
        for (int i = 0; i < nsize; i++) {
            valueD[i] = vector.get().get(i);
        }
    }

    public Vector getVector() {
        if (v == null) {
            v = new DenseVector(valueD);
        }
        return v;
    }

    public PointVector(long clusterClass, double[] value) {
        super();
        this.clusterClass = clusterClass;
        valueD = value;
    }

    public double[] getDoubleValue() {
        return valueD;
    }

    public void setValue(double[] value) {
        this.valueD = value;
    }

    public long getStrClusterClass() {
        return clusterClass;
    }

    public void setClusterClass(int clusterClass) {
        this.clusterClass = clusterClass;
    }

    public double[] getValueD() {
        return valueD;
    }

    public void setValueD(double[] valueD) {
        this.valueD = valueD;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clusterClass = in.readLong();
        int len = in.readInt();
        valueD = new double[len];
        for (int i = 0; i < len; i++) {
            valueD[i] = in.readDouble();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(clusterClass);
        out.writeInt(valueD.length);
        for (int i = 0; i < valueD.length; i++) {
            out.writeDouble(valueD[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(clusterClass).append(":");
        for (double val : valueD) {
            sb.append(val).append(" ");
        }
        return sb.toString();
    }
}
