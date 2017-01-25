package org.apache.spark.examples.my;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
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
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.spark.examples.utils.StreamUtils.streamOf;

@Slf4j
public final class YahooFinanceChart {
    private static final String URL_PATTERN = "http://real-chart.finance.yahoo.com/table.csv?s=%s&a=%s&b=%s&c=%s&d=%s&e=%s&f=%s&g=d&ignore=.csv";
    private static final String PRICES_FILE_LOCATION = "target/yahoo-finance-chart.csv";
    private static final String CSV_COLUMN_SEPARATOR = ",";
    private static final int DATE_COLUMN_NUMBER = 0;
    public static final int PRICE_COLUMN_NUMBER = 6;


    public static void main(String[] args) throws Exception {
//        SparkConf sparkConf = new SparkConf().setAppName("YahooFinanceChart");
        downloadPrices("GOOG");
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("YahooFinanceChart");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        final JavaRDD<ChartRecord> chartRecords = ctx.textFile(PRICES_FILE_LOCATION, 1)
                .mapPartitionsWithIndex(YahooFinanceChart::removeCsvHeader, false)  //skip csv header
                .mapPartitions(linesIterator -> streamOf(linesIterator)
                        .map(YahooFinanceChart::parseChartRecord)
                        .collect(Collectors.toList()))
                .cache();

        final List<Double> dailyPrices = chartRecords
                .mapPartitionsToDouble(recordsIterator -> streamOf(recordsIterator)
                        .map(ChartRecord::getPrice)
                        .collect(Collectors.toList()))
                .collect();

        final JavaPairRDD<LocalDate, ChartRecord> recordsSortedByDate = chartRecords
                .mapPartitionsToPair(recordsIterator -> streamOf(recordsIterator)
                        .map(record -> new Tuple2<>(record.getDate(), record))
                        .collect(Collectors.toList()))
                .sortByKey(false);

        final JavaPairRDD<Long, ChartRecord> recordsSortedAndEnumerated = recordsSortedByDate
                .zipWithIndex()   //enumerate Date-Record pairs
                .mapPartitionsToPair(recordsIterator -> streamOf(recordsIterator)
                        .map(tuple2LongTuple2 -> new Tuple2<>(tuple2LongTuple2._2(), tuple2LongTuple2._1()._2())) //retrieve the number of record and  the record itself
                        .collect(Collectors.toList()))
                .cache();

        final JavaPairRDD<Long, ChartRecord> yesterdayRecords = recordsSortedAndEnumerated
                .mapPartitionsToPair(linesIterator -> streamOf(linesIterator)
                        .map(enumeratedLine -> new Tuple2<>(enumeratedLine._1() - 1, enumeratedLine._2())) //shift numeration to get 'yesterday's records
                        .collect(Collectors.toList()))
                .cache();

        final JavaDoubleRDD returns = recordsSortedAndEnumerated.join(yesterdayRecords)
                .values()
                .filter(YahooFinanceChart::isReallyTodayYesterdayPair)
                .mapPartitionsToDouble(recordsPairIterator -> streamOf(recordsPairIterator)
                        .map(YahooFinanceChart::calculateReturn)
                        .collect(Collectors.toList()));

        final Double meanReturn = returns.mean();

        System.out.println(dailyPrices);
        System.out.println(meanReturn);

        ctx.stop();
    }

    private static Double calculateReturn(Tuple2<ChartRecord, ChartRecord> todayYesterdayRecords) {
        final ChartRecord todayRecord = todayYesterdayRecords._1();
        final ChartRecord yesterdayRecord = todayYesterdayRecords._2();
        return (todayRecord.getPrice() - yesterdayRecord.getPrice()) / yesterdayRecord.getPrice();
    }

    private static Boolean isReallyTodayYesterdayPair(final Tuple2<ChartRecord, ChartRecord> todayYesterday) {
        return todayYesterday._1().getDate().minusDays(1).equals(todayYesterday._2().getDate());
    }

    private static ChartRecord parseChartRecord(String line) {
        final String[] columnValues = line.split(CSV_COLUMN_SEPARATOR); //Date,Open,High,Low,Close,Volume,Adj Close
        return ChartRecord.builder()
                .date(LocalDate.parse(columnValues[DATE_COLUMN_NUMBER].trim()))
                .price(Double.parseDouble(columnValues[PRICE_COLUMN_NUMBER].trim()))
                .build();
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


    @Getter
    @AllArgsConstructor
    @Builder
    @ToString
    //TODO: It would be better to use BigDecimal for price
    private static class ChartRecord implements Serializable {
        private LocalDate date;
        private Double price;
    }
}
