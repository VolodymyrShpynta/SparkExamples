package org.apache.spark.examples.my;


import com.google.common.collect.Iterables;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.apache.spark.storage.StorageLevel.MEMORY_ONLY;

public class JavaPageRankSample {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaPairRddSample");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, List<String>> pairRDD = ctx.parallelizePairs(asList(
                new Tuple2<>("link 1", asList("link 11", "link 12")),
                new Tuple2<>("link 2", asList("link 11", "link 31", "link 21", "link 22")),
                new Tuple2<>("link 3", asList("link 11", "link 31", "link 32"))));

        JavaPairRDD<String, List<String>> links = pairRDD.partitionBy(new HashPartitioner(2))
                .persist(MEMORY_ONLY());

        JavaPairRDD<String, Double> ranks = links.mapValues(strings -> 1.0);
        JavaPairRDD<String, Tuple2<List<String>, Double>> join = links.join(ranks);
        JavaRDD<Tuple2<List<String>, Double>> values = join.values();
        JavaPairRDD<String, Double> contributions = values.flatMapToPair(tuple ->
                tuple._1().stream()
                        .map(s -> new Tuple2<>(s, tuple._2() / tuple._1().size()))
                        .collect(Collectors.toList()));
        JavaPairRDD<String, Double> resultRanks = contributions.
                reduceByKey((d1, d2) -> d1 + d2)
                .mapValues(v -> 0.15 + 0.85 * v);
        System.out.println(resultRanks.collect());
    }
}
