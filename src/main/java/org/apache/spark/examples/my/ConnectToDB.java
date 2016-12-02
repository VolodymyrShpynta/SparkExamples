package org.apache.spark.examples.my;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.JdbcRDD;
import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.sql.*;
import java.util.List;

public final class ConnectToDB {

    public static void main(String[] args) throws Exception {
        startDB();
//        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaWordCount");
        SparkContext ctx = new SparkContext(sparkConf);

        JdbcRDD<Object[]> jdbcRDD = new JdbcRDD<>(ctx,
                new DbConnection(),
                "select * from Persons where id >= ? and id <= ?",
                1,
                3,
                1,
                new MapResult(),
                ClassManifestFactory$.MODULE$.fromClass(Object[].class));
        JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD, ClassManifestFactory$.MODULE$.fromClass(Object[].class));
        List<String> collect = javaRDD.map(row -> row[0] + " " + row[1])
                .collect();
        System.out.println(collect);
        ctx.stop();
    }

    private static void startDB() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "");
        Statement statement = connection.createStatement();
        statement.execute(
                "CREATE TABLE Persons " +
                        "(" +
                        " id INT NOT NULL, " +
                        " last_name VARCHAR(255) " +
                        ");");
        statement.execute(
                "INSERT INTO Persons VALUES " +
                        "(1, 'Petrov')," +
                        "(2, 'Sydorov')," +
                        "(3, 'Ivanov')");
    }

    private static class MapResult extends AbstractFunction1<ResultSet, Object[]> implements Serializable {

        public Object[] apply(ResultSet row) {
            return JdbcRDD.resultSetToObjectArray(row);
        }
    }

    private static class DbConnection extends AbstractFunction0<Connection> implements Serializable {
        @Override
        public Connection apply() {
            try {
                return DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "");
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }
    }
}
