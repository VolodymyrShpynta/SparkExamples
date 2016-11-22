package org.apache.spark.examples.my;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by vshpynta on 18.11.16.
 */
public class JavaPairRddSample {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaPairRddSample");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaPairRDD<Integer, Integer> pairRDD = ctx.parallelizePairs(Arrays.asList(
                new Tuple2<>(1, 2),
                new Tuple2<>(3, 4),
                new Tuple2<>(3, 6)));

        System.out.println("flatMap: " + pairRDD
                        .flatMapValues(v -> listFromTo(v, 5))
                        .collect()
        );

        JavaPairRDD<Integer, Integer> otherPairRDD = ctx.parallelizePairs(Arrays.asList(
                new Tuple2<>(3, 9)));

        System.out.println("cogroup: " + pairRDD
                        .cogroup(otherPairRDD)
                        .collect()
        );
    }

    private static List<Integer> listFromTo(final int from, final int to) {
        List<Integer> list = new ArrayList<>();
        for (int i = from; i <= to; i++) {
            list.add(i);
        }
        return list;
    }
}
