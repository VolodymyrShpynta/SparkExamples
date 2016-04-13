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

package org.apache.spark.examples.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class MySparkSQLExample {
    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static void main(String[] args) throws Exception {
//        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL");
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaSparkSQL");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        HiveContext sqlContext = new HiveContext(ctx);

        sqlContext.sql("CREATE TEMPORARY TABLE people(name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'");

        sqlContext.sql("LOAD DATA LOCAL INPATH 'src/main/resources/people.txt' INTO TABLE people");
        DataFrame resultDataFrame = sqlContext.sql("SELECT * FROM people");
        resultDataFrame.show();

        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        List<String> peoples = retrieveResult(resultDataFrame);
        peoples.forEach(System.out::println);

        DataFrame teenagersDataFrame = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19");
        teenagersDataFrame.show();
        List<String> teenagers = retrieveResult(teenagersDataFrame);
        teenagers.forEach(teenager -> System.out.println("Teenager: "+teenager));

        ctx.stop();
    }

    private static List<String> retrieveResult(DataFrame result) {
        return result.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "Name: " + row.getString(0)+ ", Age:" + row.getInt(1);
            }
        }).collect();
    }
}
