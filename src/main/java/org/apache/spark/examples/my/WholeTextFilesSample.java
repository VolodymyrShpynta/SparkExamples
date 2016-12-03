package org.apache.spark.examples.my;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by volodymyr on 03.12.16.
 */
public class WholeTextFilesSample {
    public static void main(String[] args) throws Exception {
//        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        String filesPath = "src/main/resources/bucket/**/*";
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaWordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, String> pairRDD = ctx.wholeTextFiles(filesPath)
                .mapToPair(tuple -> new Tuple2<>(substring(tuple._1()), tuple._2()))
                .mapValues(WholeTextFilesSample::mapV);
        System.out.println(pairRDD.collect());
        ctx.stop();
    }

    private static String substring(String str) {
        String searchWord = "bucket";
        return str.substring(str.indexOf(searchWord) + searchWord.length());
    }

    private static String mapV(String v) {
        List<String> strings = Arrays.asList(v.split("\n"));
        System.out.println("Value: " + strings);
        return v;
    }
}
