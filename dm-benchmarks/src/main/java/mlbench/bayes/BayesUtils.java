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
package mlbench.bayes;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.training.ThetaMapper;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class BayesUtils {
	public static final Pattern SLASH = Pattern.compile("/");
	public static final String SUMMED_OBSERVATIONS = "summedObservations";
	public static final String WEIGHTS = "weights";
	private static final String DICTIONARY_FILE = "dictionary.file-";
	private static final String OUTPUT_FILES_PATTERN = "part-*";
	private static final int DICTIONARY_BYTE_OVERHEAD = 4;
	private static final String FREQUENCY_FILE = "frequency.file-";
	private static final int SEQUENCEFILE_BYTE_OVERHEAD = 45;

	static Pair<Long[], List<Path>> createDictionaryChunks(
			Path featureCountPath, Path dictionaryPathBase,
			Configuration baseConf, int chunkSizeInMegabytes)
			throws IOException {
		List<Path> chunkPaths = Lists.newArrayList();
		Configuration conf = new Configuration(baseConf);

		FileSystem fs = FileSystem.get(featureCountPath.toUri(), conf);

		long chunkSizeLimit = chunkSizeInMegabytes * 1024L * 1024L;
		int chunkIndex = 0;
		Path chunkPath = new Path(dictionaryPathBase, FREQUENCY_FILE
				+ chunkIndex);
		chunkPaths.add(chunkPath);
		SequenceFile.Writer freqWriter = new SequenceFile.Writer(fs, conf,
				chunkPath, IntWritable.class, LongWritable.class);

		try {
			long currentChunkSize = 0;
			long featureCount = 0;
			long vectorCount = Long.MAX_VALUE;
			Path filesPattern = new Path(featureCountPath, OUTPUT_FILES_PATTERN);
			for (Pair<IntWritable, LongWritable> record : new SequenceFileDirIterable<IntWritable, LongWritable>(
					filesPattern, PathType.GLOB, null, null, true, conf)) {

				if (currentChunkSize > chunkSizeLimit) {
					Closeables.close(freqWriter, false);
					chunkIndex++;

					chunkPath = new Path(dictionaryPathBase, FREQUENCY_FILE
							+ chunkIndex);
					chunkPaths.add(chunkPath);

					freqWriter = new SequenceFile.Writer(fs, conf, chunkPath,
							IntWritable.class, LongWritable.class);
					currentChunkSize = 0;
				}

				int fieldSize = SEQUENCEFILE_BYTE_OVERHEAD + Integer.SIZE / 8
						+ Long.SIZE / 8;
				currentChunkSize += fieldSize;
				IntWritable key = record.getFirst();
				LongWritable value = record.getSecond();
				if (key.get() >= 0) {
					freqWriter.append(key, value);
				} else if (key.get() == -1) {
					vectorCount = value.get();
				}
				featureCount = Math.max(key.get(), featureCount);

			}
			featureCount++;
			Long[] counts = { featureCount, vectorCount };
			return new Pair<Long[], List<Path>>(counts, chunkPaths);
		} finally {
			Closeables.close(freqWriter, false);
		}
	}

	static List<Path> createDictionaryChunks(Path wordCountPath,
			Path dictionaryPathBase, Configuration baseConf,
			int chunkSizeInMegabytes, int[] maxTermDimension)
			throws IOException {
		List<Path> chunkPaths = Lists.newArrayList();

		Configuration conf = new Configuration(baseConf);

		FileSystem fs = FileSystem.get(wordCountPath.toUri(), conf);

		long chunkSizeLimit = chunkSizeInMegabytes * 1024L * 1024L;
		int chunkIndex = 0;
		Path chunkPath = new Path(dictionaryPathBase, DICTIONARY_FILE
				+ chunkIndex);
		chunkPaths.add(chunkPath);

		SequenceFile.Writer dictWriter = new SequenceFile.Writer(fs, conf,
				chunkPath, Text.class, IntWritable.class);

		try {
			long currentChunkSize = 0;
			Path filesPattern = new Path(wordCountPath, OUTPUT_FILES_PATTERN);
			int i = 0;
			for (Pair<Writable, Writable> record : new SequenceFileDirIterable<Writable, Writable>(
					filesPattern, PathType.GLOB, null, null, true, conf)) {
				if (currentChunkSize > chunkSizeLimit) {
					Closeables.close(dictWriter, false);
					chunkIndex++;

					chunkPath = new Path(dictionaryPathBase, DICTIONARY_FILE
							+ chunkIndex);
					chunkPaths.add(chunkPath);

					dictWriter = new SequenceFile.Writer(fs, conf, chunkPath,
							Text.class, IntWritable.class);
					currentChunkSize = 0;
				}

				Writable key = record.getFirst();
				int fieldSize = DICTIONARY_BYTE_OVERHEAD
						+ key.toString().length() * 2 + Integer.SIZE / 8;
				currentChunkSize += fieldSize;
				dictWriter.append(key, new IntWritable(i++));
			}
			maxTermDimension[0] = i;
		} finally {
			Closeables.close(dictWriter, false);
		}

		return chunkPaths;
	}

	/** Write the list of labels into a map file */
	public static int writeLabelIndex(Configuration conf,
			Iterable<String> labels, Path indexPath) throws IOException {
		FileSystem fs = FileSystem.get(indexPath.toUri(), conf);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
				indexPath, Text.class, IntWritable.class);
		int i = 0;
		try {
			for (String label : labels) {
				writer.append(new Text(label), new IntWritable(i++));
			}
		} finally {
			Closeables.close(writer, false);
		}
		return i;
	}

	public static int writeLabelIndex(Configuration conf, Path indexPath,
			Iterable<Pair<Text, IntWritable>> labels) throws IOException {
		FileSystem fs = FileSystem.get(indexPath.toUri(), conf);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
				indexPath, Text.class, IntWritable.class);
		Collection<String> seen = Sets.newHashSet();
		int i = 0;
		try {
			for (Object label : labels) {
				String theLabel = SLASH.split(((Pair<?, ?>) label).getFirst()
						.toString())[1];
				if (!seen.contains(theLabel)) {
					writer.append(new Text(theLabel), new IntWritable(i++));
					seen.add(theLabel);
				}
			}
		} finally {
			Closeables.close(writer, false);
		}
		return i;
	}

	public static OpenObjectIntHashMap<String> readIndexFromCache(
			Configuration conf) throws IOException {
		OpenObjectIntHashMap<String> index = new OpenObjectIntHashMap<String>();
		for (Pair<Writable, IntWritable> entry : new SequenceFileIterable<Writable, IntWritable>(
				HadoopUtil.getSingleCachedFile(conf), conf)) {
			index.put(entry.getFirst().toString(), entry.getSecond().get());
		}
		return index;
	}

	public static NaiveBayesModel readModelFromDir(Path base, Configuration conf) {

		float alphaI = conf.getFloat(ThetaMapper.ALPHA_I, 1.0f);

		// read feature sums and label sums
		Vector scoresPerLabel = null;
		Vector scoresPerFeature = null;
		for (Pair<Text, VectorWritable> record : new SequenceFileDirIterable<Text, VectorWritable>(
				new Path(base, TrainNaiveBayesJob.WEIGHTS), PathType.LIST,
				PathFilters.partFilter(), conf)) {
			String key = record.getFirst().toString();
			VectorWritable value = record.getSecond();
			if (key.equals(TrainNaiveBayesJob.WEIGHTS_PER_FEATURE)) {
				scoresPerFeature = value.get();
			} else if (key.equals(TrainNaiveBayesJob.WEIGHTS_PER_LABEL)) {
				scoresPerLabel = value.get();
			}
		}

		// Preconditions.checkNotNull(scoresPerFeature);
		// Preconditions.checkNotNull(scoresPerLabel);

		Matrix scoresPerLabelAndFeature = new SparseMatrix(
				scoresPerLabel.size(), scoresPerFeature.size());
		for (Pair<IntWritable, VectorWritable> entry : new SequenceFileDirIterable<IntWritable, VectorWritable>(
				new Path(base, TrainNaiveBayesJob.SUMMED_OBSERVATIONS),
				PathType.LIST, PathFilters.partFilter(), conf)) {
			scoresPerLabelAndFeature.assignRow(entry.getFirst().get(), entry
					.getSecond().get());
		}

		Vector perlabelThetaNormalizer = scoresPerLabel.like();
		/*
		 * for (Pair<Text,VectorWritable> entry : new
		 * SequenceFileDirIterable<Text,VectorWritable>( new Path(base,
		 * TrainNaiveBayesJob.THETAS), PathType.LIST, PathFilters.partFilter(),
		 * conf)) { if (entry.getFirst().toString().equals(TrainNaiveBayesJob.
		 * LABEL_THETA_NORMALIZER)) { perlabelThetaNormalizer =
		 * entry.getSecond().get(); } }
		 * 
		 * Preconditions.checkNotNull(perlabelThetaNormalizer);
		 */
		return new NaiveBayesModel(scoresPerLabelAndFeature, scoresPerFeature,
				scoresPerLabel, perlabelThetaNormalizer, alphaI, false);
	}

	public static Map<Integer, String> readLabelIndex(Configuration conf,
			Path indexPath) {
		Map<Integer, String> labelMap = new HashMap<Integer, String>();
		for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(
				indexPath, true, conf)) {
			labelMap.put(pair.getSecond().get(), pair.getFirst().toString());
		}
		return labelMap;
	}

	public static void main(String args[]) throws IOException {
		Configuration conf = new JobConf(args[2]);
		NaiveBayesModel naiveBayesModel = readModelFromDir(new Path(args[0]),
				conf);
		naiveBayesModel.serialize(new Path(args[1]), conf);
	}
}
