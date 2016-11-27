package org.apache.spark.examples.my;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.examples.utils.StreamUtils.streamOf;

public class JavaMapPartitionsSample {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaPairRddSample");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> javaRDD = ctx.parallelize(Arrays.asList(1, 2, 3, 4), 2);

        JavaRDD<Integer> mapPartitionsResult = javaRDD.mapPartitions(JavaMapPartitionsSample::sum);
        System.out.println("mapPartitionsResult: " + mapPartitionsResult.collect());

        JavaRDD<Integer> mapPartitionsWithIndexResult = javaRDD.mapPartitionsWithIndex(JavaMapPartitionsSample::printPartitionWithIndex, false);
        System.out.println("mapPartitionsWithIndexResult: " + mapPartitionsWithIndexResult.collect());
    }

    private static Iterator<Integer> printPartitionWithIndex(Integer v1, Iterator<Integer> v2) {
        System.out.println("v1: " + v1);
        List<Integer> v2Collection = streamOf(v2).collect(Collectors.toList());
        System.out.println("v2: " + v2Collection);
        return v2Collection.iterator();
    }


    private static Iterable<Integer> sum(Iterator<Integer> iterator) {
        List<Integer> input = streamOf(iterator).collect(Collectors.toList());
        System.out.println("Sum: " + input);
        Integer result = input.stream()
                .reduce((int1, int2) -> int1 + int2)
                .get();
        return Collections.singletonList(result);
    }
}
