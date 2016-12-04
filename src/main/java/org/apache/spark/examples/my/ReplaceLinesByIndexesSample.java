package org.apache.spark.examples.my;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by volodymyr on 03.12.16.
 */
public class ReplaceLinesByIndexesSample {
    public static void main(String[] args) throws Exception {
//        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        String filesPath = "src/main/resources/bucket/**/*";
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("ReplaceLinesByIndexesSample");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, Long> enumeratedRdd = ctx.textFile(filesPath, 1)
                .zipWithIndex()
                .persist(StorageLevel.MEMORY_ONLY());
        enumeratedRdd.saveAsTextFile("target/indexes");
        ctx.wholeTextFiles(filesPath)
                .mapToPair(ReplaceLinesByIndexesSample::keysRelativePath)
                .flatMapValues(ReplaceLinesByIndexesSample::splitValues)
                .mapToPair(ReplaceLinesByIndexesSample::reverse)
                .join(enumeratedRdd)
                .map(Tuple2::_2)
                .mapToPair(v -> v)      //Identity function
                .groupByKey()
                .flatMapValues(v -> v)  // instead of groupByKey().flatMapValues() the sortByKey() could be used
                .mapToPair(ReplaceLinesByIndexesSample::convertToWritable)
                .saveAsHadoopFile("target/out", Text.class, LongWritable.class, MultipleOutputsByKeyFormat.class);
        ctx.stop();
    }

    private static <K, V> Tuple2<V, K> reverse(Tuple2<K, V> tuple) {
        return new Tuple2<>(tuple._2(), tuple._1());
    }

    private static Tuple2<String, String> keysRelativePath(Tuple2<String, String> tuple) {
        return new Tuple2<>(substring(tuple._1()), tuple._2());
    }

    private static Tuple2<WritableComparable, WritableComparable> convertToWritable(Tuple2<String, Long> tuple) {
        return new Tuple2<>(new Text(tuple._1()), new LongWritable(tuple._2()));
    }

    private static String substring(String str) {
        String searchWord = "bucket/";
        return str.substring(str.indexOf(searchWord) + searchWord.length());
    }

    private static List<String> splitValues(String v) {
        return Arrays.asList(v.split("\n"));
    }

    private static class MultipleOutputsByKeyFormat extends MultipleTextOutputFormat {
        @Override
        protected Object generateActualKey(Object key, Object value) {
            return NullWritable.get();
        }

        @Override
        protected String generateFileNameForKeyValue(Object key, Object value, String name) {
            return key.toString();
        }
    }
}
