package org.apache.spark.examples.utils;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by volodymyr on 27.11.16.
 */
public class StreamUtils {

    public static <T> Stream<T> streamOf(final Iterator<T> iterator) {
        final Iterable<T> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
