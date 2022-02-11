package com.cognite.client.util;

import com.cognite.client.dto.RawRow;
import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper class for working with {@link RawRow} objects.
 */
public class RawRows {

    public static RawRow of(String dbName, String tableName, String rowKey, Map<String, Object> columns) {

    }

    /**
     * Specify the target CDF Raw table (name) for a collection of {@link RawRow}.
     *
     * @param rows The rows that will get their destination table set.
     * @param tableName The CDF Raw destination table name.
     * @return the rows with table name set.
     */
    public static List<RawRow> setTableName(Collection<RawRow> rows, String tableName) {
        Preconditions.checkNotNull(rows);
        Preconditions.checkNotNull(tableName);

        return rows.stream()
                .map(row -> row.toBuilder().setTableName(tableName).build())
                .collect(Collectors.toList());
    }

    /**
     * Specify the target CDF Raw database (name) for a collection of {@link RawRow}.
     *
     * @param rows The rows that will get their destination table set.
     * @param dbName The CDF Raw destination database name.
     * @return the rows with database name set.
     */
    public static List<RawRow> setDbName(Collection<RawRow> rows, String dbName) {
        Preconditions.checkNotNull(rows);
        Preconditions.checkNotNull(dbName);

        return rows.stream()
                .map(row -> row.toBuilder().setDbName(dbName).build())
                .collect(Collectors.toList());
    }
}
