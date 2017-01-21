/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
//  Uncomment these lines to run on Cluster
//        String fileName = "README.md";
//        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        String fileName = "src/main/resources/people.txt";
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaWordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(fileName, 1);
        List<Tuple2<String, Integer>> countedWords = lines
                .flatMap(line -> Arrays.asList(SPACE.split(line)))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((i1, i2) -> i1 + i2) //count the same words (which are the keys)
                .collect();
        countedWords.forEach(wordCountTuple ->
                System.out.println(wordCountTuple._1() + ": " + wordCountTuple._2()));
        ctx.stop();
    }
}
