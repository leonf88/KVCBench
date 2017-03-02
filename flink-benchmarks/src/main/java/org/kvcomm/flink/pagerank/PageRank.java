package org.kvcomm.flink.pagerank;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

public class PageRank {

    public static void main(String[] args) throws Exception {

        int numIterations = 25;
        long numVertices = ...

        String adjacencyPath = "hdfs:///the/input/path";
        String outpath = "hdfs:///the/output/path";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, long[]>> adjacency = env.readTextFile(adjacencyPath).map(new
                AdjacencyBuilder());

        DataSet<Tuple2<Long, Double>> initialRanks = adjacency.map(new VertexInitializer(1.0 /
                numVertices));


        IterativeDataSet<Tuple2<Long, Double>> iteration = initialRanks.iterate(numIterations);

        DataSet<Tuple2<Long, Double>> newRanks = iteration
                .join(adjacency).where(0).equalTo(0).with(new RankDistributor(0.85, numVertices))
                .groupBy(0)
                .reduceGroup(new Adder());

        iteration.closeWith(newRanks).writeAsCsv(outpath, WriteMode.OVERWRITE);

//		System.out.println(env.getExecutionPlan());

        env.execute("Page Rank");
    }


    public static final class AdjacencyBuilder implements MapFunction<String, Tuple2<Long,
            long[]>> {

        @Override
        public Tuple2<Long, long[]> map(String value) throws Exception {
            String[] parts = value.split(" ");
            if (parts.length < 1) {
                throw new Exception("Malformed line: " + value);
            }

            long id = Long.parseLong(parts[0]);
            long[] targets = new long[parts.length - 1];
            for (int i = 0; i < targets.length; i++) {
                targets[i] = Long.parseLong(parts[i + 1]);
            }
            return new Tuple2<Long, long[]>(id, targets);
        }
    }

    public static final class VertexInitializer implements MapFunction<Tuple2<Long, long[]>,
            Tuple2<Long, Double>> {

        private final Double initialRank;

        public VertexInitializer(double initialRank) {
            this.initialRank = initialRank;
        }

        @Override
        public Tuple2<Long, Double> map(Tuple2<Long, long[]> value) {
            return new Tuple2<Long, Double>(value.f0, initialRank);
        }
    }

    public static final class RankDistributor implements FlatJoinFunction<Tuple2<Long, Double>,
            Tuple2<Long, long[]>, Tuple2<Long, Double>> {

        private final Tuple2<Long, Double> tuple = new Tuple2<Long, Double>();

        private final double dampeningFactor;
        private final long numVertices;


        public RankDistributor(double dampeningFactor, long numVertices) {
            this.dampeningFactor = dampeningFactor;
            this.numVertices = numVertices;
        }


        @Override
        public void join(Tuple2<Long, Double> page, Tuple2<Long, long[]> neighbors,
                         Collector<Tuple2<Long, Double>> out) {
            long[] targets = neighbors.f1;

            double rankPerTarget = dampeningFactor * page.f1 / targets.length;
            double randomJump = (1 - dampeningFactor) / numVertices;

            // emit random jump to self
            tuple.f0 = page.f0;
            tuple.f1 = randomJump;
            out.collect(tuple);

            tuple.f1 = rankPerTarget;
            for (long target : targets) {
                tuple.f0 = target;
                out.collect(tuple);
            }
        }
    }

    @ConstantFields("0")
    public static final class Adder extends RichGroupReduceFunction<Tuple2<Long, Double>,
            Tuple2<Long, Double>> {

        @Override
        public void reduce(Iterable<Tuple2<Long, Double>> values, Collector<Tuple2<Long, Double>>
                out) {
            Long key = null;
            double agg = 0.0;

            for (Tuple2<Long, Double> t : values) {
                key = t.f0;
                agg += t.f1;
            }

            out.collect(new Tuple2<Long, Double>(key, agg));
        }
    }
}
