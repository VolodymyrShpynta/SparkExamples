package org.apache.spark.examples.my;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.spark.examples.utils.StreamUtils.streamOf;

@Slf4j
public final class YahooFinanceMetrics {
    private static final String URL_PATTERN = "http://real-chart.finance.yahoo.com/table.csv?s=%s&a=%s&b=%s&c=%s&d=%s&e=%s&f=%s&g=d&ignore=.csv";
    private static final String PRICES_FILE_LOCATION = "target/yahoo-finance-metrics.csv";
    private static final String CSV_COLUMN_SEPARATOR = ",";
    public static final int PRICE_COLUMN_NUMBER = 6;


    public static void main(String[] args) throws Exception {
//        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        downloadPrices("GOOG");
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaWordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(PRICES_FILE_LOCATION, 1);
        final JavaPairRDD<String, Long> enumeratedLines = lines.zipWithIndex()
                .filter(tuple2 -> tuple2._2() > 0)  //skip csv header
                .cache();
        final List<Double> dailyPrices = enumeratedLines
                .mapPartitions(tuple2Iterator -> streamOf(tuple2Iterator)
                        .map(Tuple2::_1) //Get line
                        .map(line -> line.split(CSV_COLUMN_SEPARATOR)[PRICE_COLUMN_NUMBER].trim())  //Get price column value
                        .map(Double::valueOf)
                        .collect(Collectors.toList()))
                .collect();

        System.out.println(enumeratedLines.collect());
        System.out.println(dailyPrices);


        ctx.stop();

    }

    private static void downloadPrices(final String ticker) {
        try {
            FileUtils.copyURLToFile(constructURL(LocalDate.now(), ticker), new File(PRICES_FILE_LOCATION));
        } catch (IOException e) {
            log.error("Error downloading prices for ticker '{}'", ticker, e);
            throw new IllegalArgumentException(format("Error downloading prices for ticker '%s'", ticker), e);
        }
    }

    private static URL constructURL(final LocalDate businessDate, final String ticker) throws MalformedURLException {
        final LocalDate lastYear = businessDate.minusYears(1);

        return new URL(format(URL_PATTERN,
                ticker, lastYear.getMonthValue(), lastYear.getDayOfMonth(), lastYear.getYear(), businessDate.getMonthValue(), businessDate.getDayOfMonth(), businessDate.getYear()));
    }
}
