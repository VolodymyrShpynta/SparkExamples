package org.apache.spark.examples.my;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.util.Assert;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.spark.examples.utils.StreamUtils.streamOf;

@Slf4j
public final class YahooFinanceMetrics {
    private static final String URL_PATTERN = "http://real-chart.finance.yahoo.com/table.csv?s=%s&a=%s&b=%s&c=%s&d=%s&e=%s&f=%s&g=d&ignore=.csv";
    private static final String PRICES_FILE_LOCATION = "target/yahoo-finance-metrics.csv";
    private static final String CSV_COLUMN_SEPARATOR = ",";
    private static final int PRICE_COLUMN_NUMBER = 6;
    private static final int DATE_COLUMN_NUMBER = 0;


    public static void main(String[] args) throws Exception {
//        SparkConf sparkConf = new SparkConf().setAppName("YahooFinanceMetrics");
        downloadPrices("GOOG");
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("YahooFinanceMetrics");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(PRICES_FILE_LOCATION, 1)
                .mapPartitionsWithIndex(YahooFinanceMetrics::removeCsvHeader, false)  //skip csv header
                .cache();

        final List<Double> dailyPrices = lines
                .mapPartitions(linesIterator -> streamOf(linesIterator)
                        .map(YahooFinanceMetrics::getPrice)
                        .map(Double::valueOf)
                        .collect(Collectors.toList()))
                .collect();

        final JavaPairRDD<String, String> sortedLines = lines
                .mapPartitionsToPair(linesIterator -> streamOf(linesIterator)
                        .map(line -> new Tuple2<>(getDate(line), line))
                        .collect(Collectors.toList()))
                .sortByKey(new DateComparator(), false);

        final JavaPairRDD<Tuple2<String, String>, Long> enumeratedLines = sortedLines.zipWithIndex();

//        final JavaRDD<Tuple2<Long, String>> sortedLines = enumeratedLines.mapPartitions(tuple2Iterator -> streamOf(tuple2Iterator)
//                .map(YahooFinanceMetrics::reverseTuple) //Reverse to (Long,String) tuple
//                .collect(Collectors.toList()))
//                .cache();

        System.out.println(lines.collect());
        System.out.println(dailyPrices);
        System.out.println(sortedLines.collect());


        ctx.stop();

    }

    private static String getPrice(final String line) {
        return line.split(CSV_COLUMN_SEPARATOR)[PRICE_COLUMN_NUMBER].trim(); //Get price column value
    }

    private static String getDate(final String line) {
        return line.split(CSV_COLUMN_SEPARATOR)[DATE_COLUMN_NUMBER].trim(); //Get date column value
    }

    // Assumed there is just one header line, in the first record
    private static Iterator<String> removeCsvHeader(Integer partitionIndex, Iterator<String> partitionIterator) {
        return partitionIndex == 0 ? removeFirstLine(partitionIterator) : partitionIterator;
    }

    private static Iterator<String> removeFirstLine(Iterator<String> partitionIterator) {
        Assert.isTrue(partitionIterator.hasNext(), "There should be at least one record in the csv file");
        partitionIterator.next();  //Just skip first line
        return partitionIterator;
    }

    private static <K, V> Tuple2<V, K> reverseTuple(Tuple2<K, V> tuple) {
        return new Tuple2<>(tuple._2(), tuple._1());
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

    private static class DateComparator implements Comparator<String>, Serializable {
        @Override
        public int compare(final String dateStr1, final String dateStr2) {
            return LocalDate.parse(dateStr1).compareTo(LocalDate.parse(dateStr2));
        }
    }
}
