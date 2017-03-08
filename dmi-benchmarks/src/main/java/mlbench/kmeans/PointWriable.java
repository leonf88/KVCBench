package mlbench.kmeans;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class PointWriable implements Writable {
	// TODO change the writable comparator
	public double values[];

	PointWriable() {
	}

	PointWriable(int d) {
		values = new double[d];
		for (int i = 0; i < d; i++) {
			values[i] = 0f;
		}
	}

	PointWriable(Text str) {
		String vals[] = str.toString().split("\\s+");
		values = new double[vals.length];
		for (int i = 0; i < values.length; i++) {
			values[i] = Double.valueOf(vals[i]);
		}
	}

	public void add(PointWriable p) {
		for (int i = 0; i < values.length; i++) {
			values[i] += p.values[i];
		}
	}

	public void divide(int count) {
		for (int i = 0; i < values.length; i++) {
			values[i] /= count;
		}
	}

	public double[] getValues() {
		return values;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(values.length);
		for (double v : values) {
			out.writeDouble(v);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int w = in.readInt();
		values = new double[w];
		for (int i = 0; i < values.length; i++) {
			values[i] = in.readDouble();
		}
	}

	@Override
	public String toString() {
		return Arrays.toString(values);
	}

}
