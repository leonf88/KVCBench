/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package mlbench.kmeans;

import mpi.MPI;
import mpi.MPIException;
import mpid.core.MPI_D;
import mpid.core.MPI_D_Exception;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.InputBuffer;
import org.apache.hadoop.io.OutputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.List;

public class KmeansUtils {

	static DataInputStream readFromHDFSF(Path path, JobConf conf) {
		InputStream in = null;
		try {
			FileSystem fs = getFileSystem(conf);
			in = fs.open(path);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return new DataInputStream(in);
	}

	static OutputStream getOutputStream(String filePath, Configuration conf) {
		Path out = new Path(filePath);
		FSDataOutputStream output = null;
		try {
			if (getFileSystem(conf).exists(out)) {
				getFileSystem(conf).makeQualified(out);
			}
			output = getFileSystem(conf).create(out, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return output;
	}

	static FileSystem getFileSystem(Configuration conf) {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fs;
	}

	static void accumulate(double[] sum, double[] vals) throws MPI_D_Exception {
		if (sum.length != vals.length) {
			throw new MPI_D_Exception("Array is incorrent!");
		}
		for (int i = 0; i < sum.length; i++) {
			sum[i] += vals[i];
		}
	}

	static double distance(double[] d1, double[] d2) throws MPI_D_Exception {
		double distance = 0;
		int len = d1.length < d2.length ? d1.length : d2.length;
		for (int i = 0; i < len; i++) {
			distance += Math.abs(d1[i] - d2[i]);
			if (distance < 0) {
				throw new MPI_D_Exception("Distance is out of bound!");
			}
		}
		return distance;
	}

	static class CenterTransfer {
		private SerializationFactory serializationFactory;
		private Serializer<PointVector> serialize;
		private Deserializer<PointVector> deserializer;
		private final int INTSIZE = Integer.SIZE >> 3;
		private final int PART_BUFFER_LENGTH = 1 << 15;
		private int buffSize = -1;
		private JobConf config;
		private int rank;
		private int size;

		public CenterTransfer(JobConf conf, int rank, int size) {
			this.config = conf;
			this.rank = rank;
			this.size = size;
			this.serializationFactory = new SerializationFactory(config);
			this.serialize = serializationFactory
					.getSerializer(PointVector.class);
			this.deserializer = serializationFactory
					.getDeserializer(PointVector.class);
		}

		/**
		 * use full buffer
		 * 
		 * @param data
		 * @throws IOException
		 */
		void deserialize(byte[] data, int groupSize, List<PointVector> centers)
				throws IOException {
			for (int k = 0; k < groupSize; k++) {
				IntBuffer ib = ByteBuffer.wrap(data, k * PART_BUFFER_LENGTH,
						PART_BUFFER_LENGTH).asIntBuffer();
				int bufSize = ib.get();
				int len = ib.get();
				InputBuffer in = new InputBuffer();
				in.reset(data, k * PART_BUFFER_LENGTH + (INTSIZE << 1), len);
				deserializer.open(in);

				for (int i = 0; i < bufSize; i++) {
					PointVector point = (PointVector) ReflectionUtils
							.newInstance(PointVector.class, config);
					point = deserializer.deserialize(point);
					centers.add(point);
				}
				deserializer.close();
			}
		}

		void deserialize(byte[] data, List<PointVector> centers)
				throws IOException {
			centers.clear();
			IntBuffer ib = ByteBuffer.wrap(data, 0, PART_BUFFER_LENGTH)
					.asIntBuffer();
			int centerSize = ib.get();
			int len = ib.get();
			InputBuffer in = new InputBuffer();
			in.reset(data, INTSIZE + INTSIZE, len);
			deserializer.open(in);

			for (int i = 0; i < centerSize; i++) {
				PointVector point = (PointVector) ReflectionUtils.newInstance(
						PointVector.class, config);
				point = deserializer.deserialize(point);
				centers.add(point);
			}
			deserializer.close();
		}

		void deserialize2(byte[] data, int groupSize, List<PointVector> centers)
				throws IOException {
			for (int k = 0; k < groupSize; k++) {
				IntBuffer ib = ByteBuffer.wrap(data, k * PART_BUFFER_LENGTH,
						PART_BUFFER_LENGTH).asIntBuffer();
				int bufSize = ib.get();
				int len = ib.get();
				InputBuffer in = new InputBuffer();
				in.reset(data, k * PART_BUFFER_LENGTH + (INTSIZE << 1), len);
				deserializer.open(in);

				for (int i = 0; i < bufSize; i++) {
					PointVector point = (PointVector) ReflectionUtils
							.newInstance(PointVector.class, config);
					point = deserializer.deserialize(point);
					centers.add(point);
				}
				deserializer.close();
			}
		}

		/**
		 * use part buffer size
		 * 
		 * @return
		 * @throws IOException
		 */
		byte[] serializer(List<PointVector> centers) throws IOException {
			OutputBuffer out = new OutputBuffer();
			serialize.open(out);
			for (PointVector p : centers) {
				serialize.serialize(p);
			}
			serialize.close();
			byte[] buff = out.getData();
			int len = out.getLength();

			byte[] ds = new byte[PART_BUFFER_LENGTH];
			IntBuffer ib = ByteBuffer.wrap(ds).asIntBuffer();
			ib.put(centers.size());
			ib.put(len);
			System.arraycopy(buff, 0, ds, INTSIZE << 1, len);

			return ds;
		}

		/**
		 * gather the centers by P2p mode
		 */
		void gatherCentersByP2P(List<PointVector> centers) {
			buffSize = size * PART_BUFFER_LENGTH;

			try {
				if (rank != 0) {
					byte[] outBuffer = serializer(centers);

					MPI_D.COMM_BIPARTITE_A.Send(outBuffer, 0,
							PART_BUFFER_LENGTH, MPI.BYTE, 0, 0);
				}
				if (rank == 0) {
					byte[] inBuffer = new byte[buffSize];
					for (int i = 1; i < size; i++) {
						MPI_D.COMM_BIPARTITE_A.Recv(inBuffer, i
								* PART_BUFFER_LENGTH, PART_BUFFER_LENGTH,
								MPI.BYTE, i, 0);
					}
					deserialize(inBuffer, size, centers);
				}
			} catch (IOException | MPIException e) {
				e.printStackTrace();
			}
		}

		void broadcastCenters(List<PointVector> centers) throws IOException,
				MPIException {
			byte[] ds = new byte[PART_BUFFER_LENGTH];
			if (rank == 0) {
				OutputBuffer out = new OutputBuffer();
				serialize.open(out);
				for (PointVector p : centers) {
					serialize.serialize(p);
				}
				serialize.close();
				byte[] buff = out.getData();
				int len = out.getLength();
				IntBuffer ib = ByteBuffer.wrap(ds).asIntBuffer();
				ib.put(centers.size());
				ib.put(len);
				System.arraycopy(buff, 0, ds, INTSIZE << 1, len);
			}

			MPI_D.COMM_BIPARTITE_O
					.Bcast(ds, 0, PART_BUFFER_LENGTH, MPI.BYTE, 0);
			deserialize(ds, centers);
		}
	}

	static class EmptyReport implements Reporter {

		@Override
		public void setStatus(String status) {

		}

		@Override
		public Counters.Counter getCounter(Enum<?> name) {
			return null;
		}

		@Override
		public Counters.Counter getCounter(String group, String name) {
			return null;
		}

		@Override
		public void incrCounter(Enum<?> key, long amount) {

		}

		@Override
		public void incrCounter(String group, String counter, long amount) {

		}

		@Override
		public InputSplit getInputSplit() throws UnsupportedOperationException {
			return null;
		}

		@Override
		public float getProgress() {
			return 0;
		}

		@Override
		public void progress() {

		}
	}
}
