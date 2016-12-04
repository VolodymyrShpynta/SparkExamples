package org.apache.spark.examples.my;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.spark.SparkConf;
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
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WholeTextFilesSample");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        ctx.wholeTextFiles(filesPath)
                .mapToPair(WholeTextFilesSample::keysRelativePath)
                .flatMapValues(WholeTextFilesSample::splitValues)
                .mapToPair(WholeTextFilesSample::convertToWritable)
                .saveAsHadoopFile("target/out", Text.class, Text.class, RddMultipleTextOutputFormat.class);
        ctx.stop();
    }

    private static Tuple2<String, String> keysRelativePath(Tuple2<String, String> tuple) {
        return new Tuple2<>(substring(tuple._1()), tuple._2());
    }

    private static Tuple2<WritableComparable, WritableComparable> convertToWritable(Tuple2<String, String> tuple) {
        return new Tuple2<>(new Text(tuple._1()), new Text(tuple._2()));
    }

    private static String substring(String str) {
        String searchWord = "bucket/";
        return str.substring(str.indexOf(searchWord) + searchWord.length());
    }

    private static List<String> splitValues(String v) {
        return Arrays.asList(v.split("\n"));
    }

    private static class RddMultipleTextOutputFormat extends MultipleTextOutputFormat{
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
