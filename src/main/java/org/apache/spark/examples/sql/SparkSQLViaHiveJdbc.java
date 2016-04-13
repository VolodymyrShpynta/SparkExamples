package org.apache.spark.examples.sql;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class SparkSQLViaHiveJdbc {

    public static final String COLUMNS_DELIMITER = "\t";
    public static final String ROWS_DELIMITER = "\n";
    public static final String EMPTY_STRING = "";

    public static void main(String[] args) throws Exception {
        final String query = "SELECT * FROM people";
        final JdbcTemplate jdbcTemplate = createJdbcTemplate(createDataSource());
                jdbcTemplate.query(
                query,
                rs -> {
//                    log.debug("Query result: {}", resultSetToString(rs));  //TODO: configure logging
                    System.out.println(resultSetToString(rs));
                }
        );
    }

    private static JdbcTemplate createJdbcTemplate(final DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    private static DataSource createDataSource() {
        final DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
        dataSource.setUrl("jdbc:hive2://localhost:10000/default");
        dataSource.setUsername("vshpynta");
        dataSource.setPassword("");
        return dataSource;
    }

    private static String resultSetToString(final ResultSet rs) {
        try {
            final List<String> rowsList = new ArrayList<>();
            do {
                rowsList.add(getRowAsString(rs));
            } while (rs.next());
            return rowsList.stream().collect(Collectors.joining(ROWS_DELIMITER));
        } catch (final Exception ex) {
            log.error("Error reading ResultSet", ex);
            return EMPTY_STRING;
        }
    }

    private static String getRowAsString(final ResultSet rs) throws SQLException {
        return Stream.iterate(1, i -> i + 1)
                .limit(rs.getMetaData().getColumnCount())
                .map(columnNumber -> getColumnValue(rs, columnNumber))
                .collect(Collectors.joining(COLUMNS_DELIMITER));
    }

    private static String getColumnValue(final ResultSet rs, final int columnNumber) {
        try {
            return rs.getString(columnNumber);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
