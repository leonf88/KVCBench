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

package microbench;

import mpi.Intracomm;
import mpi.MPIException;
import mpid.core.HadoopReader;
import mpid.core.HadoopWriter;
import mpid.core.MPI_D;
import mpid.core.MPI_D_Context;
import mpid.core.MPI_D_Exception;
import mpid.core.MPI_D_Partitioner;
import mpid.core.util.MPI_D_Constants;
import mpid.util.DataMPIUtil;
import mpid.util.hadoop.HadoopIOUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.StringUtils;

import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * A TeraSort benchmark of DataMPI on HDFS with data-local feature. This
 * benchmark design is based on Hadoop TeraSort.
 */
public class TeraSortOnHDFSDataLocal {
	private static final String PARTITION_FILENAME = "_partition.lst";
	private static final String DFS_REPLICATION = "dfs.replication";
	private static final String FS_DEFALUT_NAME = "fs.default.name";
	private static final String MAPRED_INPUT_DIR = "mapred.input.dir";

	static class TextSampler implements IndexedSortable {
		private ArrayList<Text> records = new ArrayList<Text>();

		public int compare(int i, int j) {
			Text left = records.get(i);
			Text right = records.get(j);
			return left.compareTo(right);
		}

		public void swap(int i, int j) {
			Text left = records.get(i);
			Text right = records.get(j);
			records.set(j, left);
			records.set(i, right);
		}

		public void addKey(Text key) {
			records.add(new Text(key));
		}

		/**
		 * Find the split points for a given sample. The sample keys are sorted
		 * and down sampled to find even split points for the partitions. The
		 * returned keys should be the start of their respective partitions.
		 * 
		 * @param numPartitions
		 *            the desired number of partitions
		 * @return an array of size numPartitions - 1 that holds the split
		 *         points
		 */
		Text[] createPartitions(int numPartitions) {
			int numRecords = records.size();
			System.out.println("Making " + numPartitions + " from " + numRecords + " records");
			if (numPartitions > numRecords) {
				throw new IllegalArgumentException("Requested more partitions than input keys (" + numPartitions
						+ " > " + numRecords + ")");
			}
			new QuickSort().sort(this, 0, records.size());
			float stepSize = numRecords / (float) numPartitions;
			System.out.println("Step size is " + stepSize);
			Text[] result = new Text[numPartitions - 1];
			for (int i = 1; i < numPartitions; ++i) {
				result[i - 1] = records.get(Math.round(stepSize * i));
			}
			return result;
		}
	}

	/**
	 * A partitioner that splits text keys into roughly equal partitions in a
	 * global sorted order.
	 */
	static class TotalOrderPartitioner implements MPI_D_Partitioner {
		private TrieNode trie;
		private Text[] splitPoints;

		/**
		 * A generic trie node
		 */
		static abstract class TrieNode {
			private int level;

			TrieNode(int level) {
				this.level = level;
			}

			abstract int findPartition(Text key);

			abstract void print(PrintStream strm) throws IOException;

			int getLevel() {
				return level;
			}
		}

		/**
		 * An inner trie node that contains 256 children based on the next
		 * character.
		 */
		static class InnerTrieNode extends TrieNode {
			private TrieNode[] child = new TrieNode[256];

			InnerTrieNode(int level) {
				super(level);
			}

			int findPartition(Text key) {
				int level = getLevel();
				if (key.getLength() <= level) {
					return child[0].findPartition(key);
				}
				return child[(key.getBytes()[level] & 0xff)].findPartition(key);
			}

			void setChild(int idx, TrieNode child) {
				this.child[idx] = child;
			}

			void print(PrintStream strm) throws IOException {
				for (int ch = 0; ch < 255; ++ch) {
					for (int i = 0; i < 2 * getLevel(); ++i) {
						strm.print(' ');
					}
					strm.print(ch);
					strm.println(" ->");
					if (child[ch] != null) {
						child[ch].print(strm);
					}
				}
			}
		}

		/**
		 * A leaf trie node that does string compares to figure out where the
		 * given key belongs between lower..upper.
		 */
		static class LeafTrieNode extends TrieNode {
			int lower;
			int upper;
			Text[] splitPoints;

			LeafTrieNode(int level, Text[] splitPoints, int lower, int upper) {
				super(level);
				this.splitPoints = splitPoints;
				this.lower = lower;
				this.upper = upper;
			}

			int findPartition(Text key) {
				for (int i = lower; i < upper; ++i) {
					if (splitPoints[i].compareTo(key) >= 0) {
						return i;
					}
				}
				return upper;
			}

			void print(PrintStream strm) throws IOException {
				for (int i = 0; i < 2 * getLevel(); ++i) {
					strm.print(' ');
				}
				strm.print(lower);
				strm.print(", ");
				strm.println(upper);
			}
		}

		/**
		 * Read the cut points from the given sequence file.
		 * 
		 * @param fs
		 *            the file system
		 * @param p
		 *            the path to read
		 * @param job
		 *            the job config
		 * @return the strings to split the partitions on
		 * @throws IOException
		 */
		private static Text[] readPartitions(FileSystem fs, Path p, JobConf job) throws IOException {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, job);
			List<Text> parts = new ArrayList<Text>();
			Text key = new Text();
			NullWritable value = NullWritable.get();
			while (reader.next(key, value)) {
				parts.add(key);
				key = new Text();
			}
			reader.close();
			return parts.toArray(new Text[parts.size()]);
		}

		/**
		 * Given a sorted set of cut points, build a trie that will find the
		 * correct partition quickly.
		 * 
		 * @param splits
		 *            the list of cut points
		 * @param lower
		 *            the lower bound of partitions 0..numPartitions-1
		 * @param upper
		 *            the upper bound of partitions 0..numPartitions-1
		 * @param prefix
		 *            the prefix that we have already checked against
		 * @param maxDepth
		 *            the maximum depth we will build a trie for
		 * @return the trie node that will divide the splits correctly
		 */
		private static TrieNode buildTrie(Text[] splits, int lower, int upper, Text prefix, int maxDepth) {
			int depth = prefix.getLength();
			if (depth >= maxDepth || lower == upper) {
				return new LeafTrieNode(depth, splits, lower, upper);
			}
			InnerTrieNode result = new InnerTrieNode(depth);
			Text trial = new Text(prefix);
			// append an extra byte on to the prefix
			trial.append(new byte[1], 0, 1);
			int currentBound = lower;
			for (int ch = 0; ch < 255; ++ch) {
				trial.getBytes()[depth] = (byte) (ch + 1);
				lower = currentBound;
				while (currentBound < upper) {
					if (splits[currentBound].compareTo(trial) >= 0) {
						break;
					}
					currentBound += 1;
				}
				trial.getBytes()[depth] = (byte) ch;
				result.child[ch] = buildTrie(splits, lower, currentBound, trial, maxDepth);
			}
			// pick up the rest
			trial.getBytes()[depth] = 127;
			result.child[255] = buildTrie(splits, currentBound, upper, trial, maxDepth);
			return result;
		}

		public TotalOrderPartitioner() {
		}

		@Override
		public int getPartition(Object key, Object value, int numPartitions) throws MPI_D_Exception {
			return trie.findPartition((Text) key);
		}

		@Override
		public void configure(MPI_D_Context context) throws MPI_D_Exception {
			try {
				JobConf conf = context.getJobConf();
				conf.set(DFS_REPLICATION, "1");
				Intracomm comm = MPI_D.COMM_BIPARTITE_O != null ? MPI_D.COMM_BIPARTITE_O : MPI_D.COMM_BIPARTITE_A;

				if (comm.Rank() == 0) {
					Path partitionFile = new Path(outDir, PARTITION_FILENAME);
					URI partitionUri = new URI(partitionFile.toString() + "#" + PARTITION_FILENAME);
					setInputPaths(conf, new Path(inDir));
					TeraInputFormat.writePartitionFile(conf, partitionFile);
					DistributedCache.addCacheFile(partitionUri, conf);
					DistributedCache.createSymlink(conf);
				}
				comm.Barrier();

				FileSystem fs = FileSystem.get(conf);
				Path partFile = new Path(outDir, PARTITION_FILENAME);
				splitPoints = readPartitions(fs, partFile, conf);
				trie = buildTrie(splitPoints, 0, splitPoints.length, new Text(), 2);
			} catch (IOException ie) {
				throw new IllegalArgumentException("can't read paritions file", ie);
			} catch (MPIException e) {
				e.printStackTrace();
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}
	}

	public static void setInputPaths(JobConf conf, Path... inputPaths) {
		Path path = new Path(conf.getWorkingDirectory(), inputPaths[0]);
		StringBuffer str = new StringBuffer(StringUtils.escapeString(path.toString()));
		for (int i = 1; i < inputPaths.length; i++) {
			str.append(StringUtils.COMMA_STR);
			path = new Path(conf.getWorkingDirectory(), inputPaths[i]);
			str.append(StringUtils.escapeString(path.toString()));
		}
		conf.set(MAPRED_INPUT_DIR, str.toString());
	}

	private static String confPath = null;
	private static String inDir = null, outDir = null, maxUsedMemPercent = null, partSize = null, outFileNum = null,
			spillPercent = null;

	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException, MPIException {
		try {
			if (!TaskAttemptContext.class.isInterface()) {
				throw new IOException("Currently TeraSort benchmark is supported under Hadoop-2.x runtime");
			}

			parseArgs(args);
			HashMap<String, String> conf = new HashMap<String, String>();
			initConf(conf);
			JobConf jobConf = new JobConf(confPath);
			conf.put(FS_DEFALUT_NAME, jobConf.get(FS_DEFALUT_NAME));
			
			MPI_D.Init(args, MPI_D.Mode.Common, conf);

			if (MPI_D.COMM_BIPARTITE_O != null) {
				// O communicator
				int rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_O);
				if (rank == 0) {
					System.out.println(TeraSortOnHDFSDataLocal.class.getSimpleName() + " O start.");
					DataMPIUtil.printArgs(args);
				}

				HadoopReader<Text, Text> reader = HadoopIOUtil.getReader(jobConf, inDir, TeraInputFormat.class, rank,
						MPI_D.COMM_BIPARTITE_O);
				Text khead = reader.createKey();
				Text vhead = reader.createValue();
				while (reader.next(khead, vhead)) {
					// send key-value
					MPI_D.Send(khead, vhead);
				}
				reader.close();
			} else if (MPI_D.COMM_BIPARTITE_A != null) {
				// A communicator
				int rank = MPI_D.Comm_rank(MPI_D.COMM_BIPARTITE_A);
				if (rank == 0) {
					System.out.println(TeraSortOnHDFSDataLocal.class.getSimpleName() + " A start.");
				}

				HadoopWriter<Text, Text> outrw = HadoopIOUtil.getNewWriter(jobConf, outDir, Text.class, Text.class,
						TeraOutputFormat.class, null, rank, MPI_D.COMM_BIPARTITE_A);
				// recv key-value.
				Object[] keyValue = MPI_D.Recv();
				while (keyValue != null) {
					outrw.write((Text) keyValue[0], (Text) keyValue[1]);
					keyValue = MPI_D.Recv();
				}
				outrw.close();
			}
			MPI_D.Finalize();
		} catch (MPI_D_Exception e) {
			e.printStackTrace();
		}
	}

	private static void initConf(HashMap<String, String> conf) {
		conf.put(MPI_D_Constants.ReservedKeys.KEY_CLASS, org.apache.hadoop.io.Text.class.getName());
		conf.put(MPI_D_Constants.ReservedKeys.VALUE_CLASS, org.apache.hadoop.io.Text.class.getName());
		conf.put(MPI_D_Constants.ReservedKeys.PARTITIONER_CLASS, TotalOrderPartitioner.class.getName());

		if (maxUsedMemPercent != null) {
			conf.put(MPI_D_Constants.ReservedKeys.CommonModeKeys.MAX_MEM_USED_PERCENT, maxUsedMemPercent);
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
	}

	private static void parseArgs(String[] args) {
		if (args.length < 3) {
			System.err.println("ERROR: Wrong number of parameters: " + args.length + " instead of 3.");
			System.err.println("Usage: terasort <core-site-path> <in> <out>");
			System.exit(-1);
		} else if (args.length == 3) {
			confPath = args[0];
			inDir = args[1];
			outDir = args[2];
		} else if (args.length <= 7) {
			// Advanced Usage
			confPath = args[0];
			inDir = args[1];
			outDir = args[2];
			maxUsedMemPercent = args[3];
			partSize = args[4];
			outFileNum = args[5];
			spillPercent = args[6];
		} else {
			System.err.println("ERROR: Error number of parameters.");
			System.exit(-1);
		}
	}
}

class TeraInputFormat extends FileInputFormat<Text, Text> {

	static final String PARTITION_FILENAME = "_partition.lst";
	static final String SAMPLE_SIZE = "terasort.partitions.sample";
	private static JobConf lastConf = null;
	private static InputSplit[] lastResult = null;

	static class TextSampler implements IndexedSortable {
		private ArrayList<Text> records = new ArrayList<Text>();

		public int compare(int i, int j) {
			Text left = records.get(i);
			Text right = records.get(j);
			return left.compareTo(right);
		}

		public void swap(int i, int j) {
			Text left = records.get(i);
			Text right = records.get(j);
			records.set(j, left);
			records.set(i, right);
		}

		public void addKey(Text key) {
			records.add(new Text(key));
		}

		/**
		 * Find the split points for a given sample. The sample keys are sorted
		 * and down sampled to find even split points for the partitions. The
		 * returned keys should be the start of their respective partitions.
		 * 
		 * @param numPartitions
		 *            the desired number of partitions
		 * @return an array of size numPartitions - 1 that holds the split
		 *         points
		 */
		Text[] createPartitions(int numPartitions) {
			int numRecords = records.size();
			System.out.println("Making " + numPartitions + " from " + numRecords + " records");
			if (numPartitions > numRecords) {
				throw new IllegalArgumentException("Requested more partitions than input keys (" + numPartitions
						+ " > " + numRecords + ")");
			}
			new QuickSort().sort(this, 0, records.size());
			float stepSize = numRecords / (float) numPartitions;
			System.out.println("Step size is " + stepSize);
			Text[] result = new Text[numPartitions - 1];
			for (int i = 1; i < numPartitions; ++i) {
				result[i - 1] = records.get(Math.round(stepSize * i));
			}
			return result;
		}
	}

	/**
	 * Use the input splits to take samples of the input and generate sample
	 * keys. By default reads 100,000 keys from 10 locations in the input, sorts
	 * them and picks N-1 keys to generate N equally sized partitions.
	 * 
	 * @param conf
	 *            the job to sample
	 * @param partFile
	 *            where to write the output file to
	 * @throws IOException
	 *             if something goes wrong
	 */
	public static void writePartitionFile(JobConf conf, Path partFile) throws IOException {
		TeraInputFormat inFormat = new TeraInputFormat();
		TextSampler sampler = new TextSampler();
		Text key = new Text();
		Text value = new Text();
		int partitions = conf.getNumReduceTasks();
		long sampleSize = conf.getLong(SAMPLE_SIZE, 100000);
		InputSplit[] splits = inFormat.getSplits(conf, conf.getNumMapTasks());
		int samples = Math.min(10, splits.length);
		long recordsPerSample = sampleSize / samples;
		int sampleStep = splits.length / samples;
		long records = 0;
		// take N samples from different parts of the input
		for (int i = 0; i < samples; ++i) {
			RecordReader<Text, Text> reader = inFormat.getRecordReader(splits[sampleStep * i], conf, null);
			while (reader.next(key, value)) {
				sampler.addKey(key);
				records += 1;
				if ((i + 1) * recordsPerSample <= records) {
					break;
				}
			}
		}
		FileSystem outFs = partFile.getFileSystem(conf);
		if (outFs.exists(partFile)) {
			outFs.delete(partFile, false);
		}
		SequenceFile.Writer writer = SequenceFile.createWriter(outFs, conf, partFile, Text.class, NullWritable.class);
		NullWritable nullValue = NullWritable.get();
		for (Text split : sampler.createPartitions(partitions)) {
			writer.append(split, nullValue);
		}
		writer.close();
	}

	static class TeraRecordReader implements RecordReader<Text, Text> {
		private static final int KEY_LENGTH = 10;
		private static final int VALUE_LENGTH = 90;
		private static final int RECORD_LENGTH = KEY_LENGTH + VALUE_LENGTH;
		private FSDataInputStream in;
		private long offset;
		private long length;
		private byte[] buffer = new byte[RECORD_LENGTH];

		public TeraRecordReader(Configuration job, FileSplit split) throws IOException {
			Path p = ((FileSplit) split).getPath();
			FileSystem fs = p.getFileSystem(job);
			in = fs.open(p);
			long start = ((FileSplit) split).getStart();
			// find the offset to start at a record boundary
			offset = (RECORD_LENGTH - (start % RECORD_LENGTH)) % RECORD_LENGTH;
			in.seek(start + offset);
			length = ((FileSplit) split).getLength();
		}

		public void close() throws IOException {
			in.close();
		}

		public float getProgress() throws IOException {
			return (float) offset / length;
		}

		@Override
		public Text createKey() {
			return new Text();
		}

		@Override
		public Text createValue() {
			return new Text();
		}

		@Override
		public long getPos() throws IOException {
			return in.getPos();
		}

		@Override
		public boolean next(Text key, Text value) throws IOException {
			if (offset >= length) {
				return false;
			}
			int read = 0;
			while (read < RECORD_LENGTH) {
				long newRead = in.read(buffer, read, RECORD_LENGTH - read);
				if (newRead == -1) {
					if (read == 0) {
						return false;
					} else {
						throw new EOFException("read past eof");
					}
				}
				read += newRead;
			}
			key.set(buffer, 0, KEY_LENGTH);
			value.set(buffer, KEY_LENGTH, VALUE_LENGTH);
			offset += RECORD_LENGTH;
			return true;
		}
	}

	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		return new TeraRecordReader(job, (FileSplit) split);
	}

	@Override
	public InputSplit[] getSplits(JobConf conf, int splits) throws IOException {
		if (conf == lastConf) {
			return lastResult;
		}
		lastConf = conf;
		lastResult = super.getSplits(conf, splits);
		return lastResult;
	}
}

class TeraOutputFormat extends FileOutputFormat<Text, Text> {
	static final String FINAL_SYNC_ATTRIBUTE = "mapreduce.terasort.final.sync";
	private OutputCommitter committer = null;

	/**
	 * Set the requirement for a final sync before the stream is closed.
	 */
	static void setFinalSync(JobContext job, boolean newValue) {
		job.getConfiguration().setBoolean(FINAL_SYNC_ATTRIBUTE, newValue);
	}

	/**
	 * Does the user want a final sync at close?
	 */
	public static boolean getFinalSync(JobContext job) {
		return job.getConfiguration().getBoolean(FINAL_SYNC_ATTRIBUTE, false);
	}

	static class TeraRecordWriter extends RecordWriter<Text, Text> {
		private boolean finalSync = false;
		private FSDataOutputStream out;

		public TeraRecordWriter(FSDataOutputStream out, JobContext job) {
			finalSync = getFinalSync(job);
			this.out = out;
		}

		public synchronized void write(Text key, Text value) throws IOException {
			out.write(key.getBytes(), 0, key.getLength());
			out.write(value.getBytes(), 0, value.getLength());
		}

		public void close(TaskAttemptContext context) throws IOException {
			if (finalSync) {
				out.sync();
			}
			out.close();
		}
	}

	@Override
	public void checkOutputSpecs(JobContext job) throws InvalidJobConfException, IOException {
		// Ensure that the output directory is set
		Path outDir = getOutputPath(job);
		if (outDir == null) {
			throw new InvalidJobConfException("Output directory not set in JobConf.");
		}

		// get delegation token for outDir's file system
		TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[] { outDir }, job.getConfiguration());
	}

	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException {
		Path file = getDefaultWorkFile(job, "");
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		FSDataOutputStream fileOut = fs.create(file);
		return new TeraRecordWriter(fileOut, job);
	}

	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
		if (committer == null) {
			Path output = getOutputPath(context);
			committer = new FileOutputCommitter(output, context);
		}
		return committer;
	}

}