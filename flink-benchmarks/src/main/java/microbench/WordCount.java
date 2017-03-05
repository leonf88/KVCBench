package microbench;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.StringTokenizer;

public class WordCount {

    public static void main(String[] args) throws Exception {
        final String inputPath = args[0];
        final String outputPath = args[1];

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Setup Hadoop’s TextInputFormat
        HadoopInputFormat<LongWritable, Text> hadoopInputFormat = new HadoopInputFormat<>(
                new TextInputFormat(), LongWritable.class, Text.class, new JobConf());
        TextInputFormat.addInputPath(hadoopInputFormat.getJobConf(), new Path(inputPath));

        // Read a DataSet with the Hadoop InputFormat
        DataSet<Tuple2<LongWritable, Text>> text = env.createInput(hadoopInputFormat);
        DataSet<Tuple2<Text, LongWritable>> words = text
                // Wrap Tokenizer Mapper function
                .flatMap(new FlatMapFunction<Tuple2<LongWritable, Text>, Tuple2<Text,
                        LongWritable>>() {
                    private final LongWritable one = new LongWritable(1);
                    private Text word = new Text();

                    @Override
                    public void flatMap(Tuple2<LongWritable, Text> tuple,
                                        Collector<Tuple2<Text, LongWritable>> context) throws
                            Exception {
                        StringTokenizer itr = new StringTokenizer(tuple.f1.toString());
                        while (itr.hasMoreTokens()) {
                            word.set(itr.nextToken());
                            context.collect(new Tuple2<>(word, one));
                        }
                    }
                })
                .groupBy(0)
                .aggregate(Aggregations.SUM, 1);

        // Setup Hadoop’s TextOutputFormat
        HadoopOutputFormat<Text, LongWritable> hadoopOutputFormat = new HadoopOutputFormat<>(
                new TextOutputFormat<Text, LongWritable>(), new JobConf());
        hadoopOutputFormat.getJobConf().set("mapred.textoutputformat.separator", " ");
        TextOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf(), new Path(outputPath));

        // Output & Execute
        words.output(hadoopOutputFormat);
        env.execute("Hadoop Compat WordCount");
    }
}
//    public static void main(String[] args) throws Exception {
//        long start = System.currentTimeMillis();
//
//        if (args.length != 4) {
//            System.out.println("Usage: WordCount hdfs inputPath outputPath #partitions ");
//            return;
//        }
//        String hdfs = args[0];
//        String inputPath = hdfs + args[1];
//        String outputPath = hdfs + args[2];
//        int partitions = Integer.parseInt(args[3]);
//
//        JobConf mapredConf = new JobConf();
//        mapredConf.set("fs.defaultFS", hdfs);
//        mapredConf.set("mapreduce.input.fileinputformat.inputdir", inputPath);
//        mapredConf.set("mapreduce.output.fileoutputformat.outputdir", outputPath);
//        mapredConf.setInt("mapreduce.job.reduces", partitions);
//
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//        DataSet<String> text = env.readTextFile(inputPath);
//        DataSet<Tuple2<String, Integer>> wordCountResults = text.flatMap(new LineSplitter())
//                .groupBy(0)
//                .aggregate(Aggregations.SUM, 1);
////        DataSource<Tuple2<LongWritable, Text>> data = env.readHadoopFile(
////                new TextInputFormat(), LongWritable.class, Text.class, inputPath, mapredConf);
////        DataSet<Tuple2<String, Integer>> wordCountResults = data.flatMap(new LineSplitter())
////                .groupBy(0)
////                .aggregate(Aggregations.SUM, 1);
////        List<Tuple2<Text, IntWritable>> collect = wordCountResults.collect();
//        wordCountResults.output(new HadoopOutputFormat<>(new TextOutputFormat<>(), mapredConf));
//
//        long end = System.currentTimeMillis();
//
//        System.out.println("\n Time: " + (end - start));
//    }

//private static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
//    @Override
//    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
//        StringTokenizer itr = new StringTokenizer(value);
//        while (itr.hasMoreTokens()) {
//            out.collect(new Tuple2<>(itr.nextToken(), 1));
//        }
//    }
//}
