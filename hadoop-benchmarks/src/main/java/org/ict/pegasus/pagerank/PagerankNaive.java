package org.ict.pegasus.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class PagerankNaive extends Configured implements Tool {
    protected static double converge_threshold = 1.0E-6D;
    protected Path edge_path = null;
    protected Path vector_path = null;
    protected Path tempmv_path = null;
    protected Path output_path = null;
    protected String local_output_path;
    protected int number_nodes = 0;
    protected int niteration = 1;
    protected double mixing_c = 0.8500000238418579D;
    protected int nreducers = 1;
    protected int make_symmetric = 0;

    public PagerankNaive() {
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new PagerankNaive(), args);
        System.exit(result);
    }

    protected static int printUsage() {
        System.out.println("PagerankNaive <edge_path> <vec_path> <temppr_path> <output_path> " +
                "<# of nodes>  <# of tasks> <max iteration> <makesym or nosym> <new or contNN>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public int run(String[] args) throws Exception {
        if (args.length != 9) {
            return printUsage();
        } else {
            this.edge_path = new Path(args[0]);
            this.vector_path = new Path(args[1]);
            this.tempmv_path = new Path(args[2]);
            this.output_path = new Path(args[3]);
            this.number_nodes = Integer.parseInt(args[4]);
            this.nreducers = Integer.parseInt(args[5]);
            this.niteration = Integer.parseInt(args[6]);
            if (args[7].compareTo("makesym") == 0) {
                this.make_symmetric = 1;
            } else {
                this.make_symmetric = 0;
            }

            int cur_iteration = 1;
            if (args[8].startsWith("cont")) {
                cur_iteration = Integer.parseInt(args[7].substring(4));
            }

            this.local_output_path = args[2] + "_temp";
            converge_threshold = 1.0D / (double) this.number_nodes / 10.0D;
            System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
            System.out.println("[PEGASUS] Computing PageRank. Max iteration = " + this.niteration
                    + ", threshold = " + converge_threshold + ", cur_iteration=" + cur_iteration
                    + "\n");
            FileSystem fs = FileSystem.get(this.getConf());
            ArrayList iterTimes = new ArrayList();

            int i;
            for (i = cur_iteration; i <= this.niteration; ++i) {
                long j = System.currentTimeMillis();
                JobClient.runJob(this.configStage1());
                RunningJob job = JobClient.runJob(this.configStage2());
                Counters c = job.getCounters();
                long changed = c.getCounter(PagerankNaive.PrCounters.CONVERGE_CHECK);
                System.out.println("Iteration = " + i + ", changed reducer = " + changed);
                fs.delete(this.vector_path);
                fs.delete(this.tempmv_path);
                if (i != this.niteration) {
                    fs.rename(this.output_path, this.vector_path);
                }

                iterTimes.add(Long.valueOf(System.currentTimeMillis() - j));
            }

            if (i == this.niteration) {
                System.out.println("Reached the max iteration. Now preparing to finish...");
            }

            for (int j = 0; j < iterTimes.size(); ++j) {
                System.out.println("Iteration[" + j + "] cost " + iterTimes.get(j) + " ms.");
            }

            System.out.println("\n[PEGASUS] PageRank computed.");
            System.out.println("[PEGASUS] The final PageRanks are in the HDFS pr_vector.");
            return 0;
        }
    }

    protected JobConf configStage1() throws Exception {
        JobConf conf = new JobConf(this.getConf(), PagerankNaive.class);
        conf.set("number_nodes", "" + this.number_nodes);
        conf.set("mixing_c", "" + this.mixing_c);
        conf.set("make_symmetric", "" + this.make_symmetric);
        conf.setJobName("Pagerank_Stage1");
        conf.setMapperClass(PagerankNaive.MapStage1.class);
        conf.setReducerClass(PagerankNaive.RedStage1.class);
        FileInputFormat.setInputPaths(conf, new Path[]{this.edge_path, this.vector_path});
        FileOutputFormat.setOutputPath(conf, this.tempmv_path);
        conf.setNumReduceTasks(this.nreducers);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        return conf;
    }

    protected JobConf configStage2() throws Exception {
        JobConf conf = new JobConf(this.getConf(), PagerankNaive.class);
        conf.set("number_nodes", "" + this.number_nodes);
        conf.set("mixing_c", "" + this.mixing_c);
        conf.set("converge_threshold", "" + converge_threshold);
        conf.setJobName("Pagerank_Stage2");
        conf.setMapperClass(PagerankNaive.MapStage2.class);
        conf.setReducerClass(PagerankNaive.RedStage2.class);
        FileInputFormat.setInputPaths(conf, new Path[]{this.tempmv_path});
        FileOutputFormat.setOutputPath(conf, this.output_path);
        conf.setNumReduceTasks(this.nreducers);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        return conf;
    }

    public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text,
            IntWritable, Text> {
        int make_symmetric = 0;

        public MapStage1() {
        }

        public void configure(JobConf job) {
            this.make_symmetric = Integer.parseInt(job.get("make_symmetric"));
            System.out.println("MapStage1 : make_symmetric = " + this.make_symmetric);
        }

        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output,
                        Reporter reporter) throws IOException {
            String line_text = value.toString();
            if (!line_text.startsWith("#")) {
                String[] line = line_text.split("\t");
                if (line.length < 2)
                    return;

                if (line[1].charAt(0) == 'v') {
                    output.collect(new IntWritable(Integer.parseInt(line[0])), new Text(line[1]));
                } else {
                    int src_id = Integer.parseInt(line[0]);
                    int dst_id = Integer.parseInt(line[1]);
                    output.collect(new IntWritable(src_id), new Text(line[1]));
                    if (this.make_symmetric == 1) {
                        output.collect(new IntWritable(dst_id), new Text(line[0]));
                    }
                }
            }
        }
    }

    public static class MapStage2 extends MapReduceBase implements Mapper<LongWritable, Text,
            IntWritable, Text> {
        public MapStage2() {
        }

        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output,
                        Reporter reporter) throws IOException {
            String[] line = value.toString().split("\t");
            output.collect(new IntWritable(Integer.parseInt(line[0])), new Text(line[1]));
        }
    }

    protected static enum PrCounters {
        CONVERGE_CHECK;

        private PrCounters() {
        }
    }

    public static class RedStage1 extends MapReduceBase implements Reducer<IntWritable, Text,
            IntWritable, Text> {
        int number_nodes = 0;
        double mixing_c = 0.0D;
        double random_coeff = 0.0D;

        public RedStage1() {
        }

        public void configure(JobConf job) {
            this.number_nodes = Integer.parseInt(job.get("number_nodes"));
            this.mixing_c = Double.parseDouble(job.get("mixing_c"));
            this.random_coeff = (1.0D - this.mixing_c) / (double) this.number_nodes;
            System.out.println("RedStage1: number_nodes = " + this.number_nodes + ", mixing_c = "
                    + this.mixing_c + ", random_coeff = " + this.random_coeff);
        }

        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable,
                Text> output, Reporter reporter) throws IOException {
            double cur_rank = 0.0D;
            ArrayList dst_nodes_list = new ArrayList();

            while (values.hasNext()) {
                String outdeg = ((Text) values.next()).toString();
                String[] line = outdeg.split("\t");
                if (line.length == 1) {
                    if (outdeg.charAt(0) == 'v') {
                        cur_rank = Double.parseDouble(outdeg.substring(1));
                    } else {
                        dst_nodes_list.add(Integer.valueOf(Integer.parseInt(line[0])));
                    }
                }
            }

            output.collect(key, new Text("s" + cur_rank));
            int var11 = dst_nodes_list.size();
            if (var11 > 0) {
                cur_rank /= (double) var11;
            }

            for (int i = 0; i < var11; ++i) {
                output.collect(new IntWritable(((Integer) dst_nodes_list.get(i)).intValue()), new
                        Text("v" + cur_rank));
            }

        }
    }

    public static class RedStage2 extends MapReduceBase implements Reducer<IntWritable, Text,
            IntWritable, Text> {
        int number_nodes = 0;
        double mixing_c = 0.0D;
        double random_coeff = 0.0D;
        double converge_threshold = 0.0D;
        int change_reported = 0;

        public RedStage2() {
        }

        public void configure(JobConf job) {
            this.number_nodes = Integer.parseInt(job.get("number_nodes"));
            this.mixing_c = Double.parseDouble(job.get("mixing_c"));
            this.random_coeff = (1.0D - this.mixing_c) / (double) this.number_nodes;
            this.converge_threshold = Double.parseDouble(job.get("converge_threshold"));
            System.out.println("RedStage2: number_nodes = " + this.number_nodes + ", mixing_c = "
                    + this.mixing_c + ", random_coeff = " + this.random_coeff + ", " +
                    "converge_threshold = " + this.converge_threshold);
        }

        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable,
                Text> output, Reporter reporter) throws IOException {
            double next_rank = 0.0D;
            double previous_rank = 0.0D;

            while (values.hasNext()) {
                String diff = ((Text) values.next()).toString();
                if (diff.charAt(0) == 's') {
                    previous_rank = Double.parseDouble(diff.substring(1));
                } else {
                    next_rank += Double.parseDouble(diff.substring(1));
                }
            }

            next_rank = next_rank * this.mixing_c + this.random_coeff;
            output.collect(key, new Text("v" + next_rank));
            if (this.change_reported == 0) {
                double diff1 = Math.abs(previous_rank - next_rank);
                if (diff1 > this.converge_threshold) {
                    reporter.incrCounter(PagerankNaive.PrCounters.CONVERGE_CHECK, 1L);
                    this.change_reported = 1;
                }
            }

        }
    }
}
