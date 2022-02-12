package com.cognite.client.util;

import com.cognite.client.dto.RawRow;
import com.cognite.client.servicesV1.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper class for working with {@link RawRow} objects.
 */
public class RawRows {

    /**
     * Create a {@link RawRow} object based on a {@code Map<String, Object>} representing the row columns.
     *
     * The {@code Map<String, Object>} is parsed into a {@code Struct} and set as the {@code columns} in the {@link RawRow}.
     * Each entry in the {@code Map} becomes a separate column in the resulting {@link RawRow}.
     *
     * You need to add a valid database, table name and row key to the resulting {@link RawRow} afterwards. This method will just
     * set them as empty strings.
     *
     * @param columns The {@code Map} representing the row columns.
     * @return The resulting {@code RawRow} object.
     * @throws Exception if the input Map cannot be parsed correctly.
     */
    public static RawRow of(Map<String, Object> columns) throws Exception {
        return RawRows.of(
                "",
                columns);
    }

    /**
     * Create a {@link RawRow} object based on a {@code Map<String, Object>} representing the row columns.
     *
     * The {@code Map<String, Object>} is parsed into a {@code Struct} and set as the {@code columns} in the {@link RawRow}.
     * Each entry in the {@code Map} becomes a separate column in the resulting {@link RawRow}.
     *
     * You need to add a valid database and table name to the resulting {@link RawRow} afterwards. This method will just
     * set them as empty strings.
     *
     * @param rowKey The row key to add to the {@code RawRow}.
     * @param columns The {@code Map} representing the row columns.
     * @return The resulting {@code RawRow} object.
     * @throws Exception if the input Map cannot be parsed correctly.
     */
    public static RawRow of(String rowKey, Map<String, Object> columns) throws Exception {
        return RawRows.of(
                "",
                "",
                rowKey,
                columns);
    }

    /**
     * Create a {@link RawRow} object based on a {@code Map<String, Object>} representing the row columns.
     *
     * The {@code Map<String, Object>} is parsed into a {@code Struct} and set as the {@code columns} in the {@link RawRow}.
     * Each entry in the {@code Map} becomes a separate column in the resulting {@link RawRow}.
     *
     * @param dbName The database name to add to the {@code RawRow}.
     * @param tableName The table name to add to the {@code RawRow}.
     * @param rowKey The row key to add to the {@code RawRow}.
     * @param columns The {@code Map} representing the row columns.
     * @return The resulting {@code RawRow} object.
     * @throws Exception if the input Map cannot be parsed correctly.
     */
    public static RawRow of(String dbName, String tableName, String rowKey, Map<String, Object> columns) throws Exception {
        // First, convert the columns to Json. We use that as an intermediate, generic format.
        ObjectMapper mapper = JsonUtil.getObjectMapperInstance();
        String jsonString = mapper.writeValueAsString(columns);

        // Then, parse the Json representation
        return RawRows.of(
                dbName,
                tableName,
                rowKey,
                jsonString);
    }

    /**
     * Create a {@link RawRow} object based on a {@code Json object} representing the row columns.
     *
     * The {@code Json object} is parsed into a {@code Struct} and set as the {@code columns} in the {@link RawRow}.
     * Each top-level field in the {@code Json object} becomes a separate column in the resulting {@link RawRow}.
     *
     * You need to add a valid database, table name and row key to the resulting {@link RawRow} afterwards. This method will just
     * set them as empty strings.
     *
     * @param columnsJsonObject The {@code Json object} representing the row columns.
     * @return The resulting {@code RawRow} object.
     * @throws Exception if the input Json does not represent a valid Json object.
     */
    public static RawRow of(String columnsJsonObject) throws Exception {
        return RawRows.of(
                "",
                columnsJsonObject);
    }

    /**
     * Create a {@link RawRow} object based on a {@code Json object} representing the row columns.
     *
     * The {@code Json object} is parsed into a {@code Struct} and set as the {@code columns} in the {@link RawRow}.
     * Each top-level field in the {@code Json object} becomes a separate column in the resulting {@link RawRow}.
     *
     * You need to add a valid database and table name to the resulting {@link RawRow} afterwards. This method will just
     * set them as empty strings.
     *
     * @param rowKey The row key to add to the {@code RawRow}.
     * @param columnsJsonObject The {@code Json object} representing the row columns.
     * @return The resulting {@code RawRow} object.
     * @throws Exception if the input Json does not represent a valid Json object.
     */
    public static RawRow of(String rowKey, String columnsJsonObject) throws Exception {
        return RawRows.of(
                "",
                "",
                rowKey,
                columnsJsonObject);
    }

    /**
     * Create a {@link RawRow} object based on a {@code Json object} representing the row columns.
     *
     * The {@code Json object} is parsed into a {@code Struct} and set as the {@code columns} in the {@link RawRow}.
     * Each top-level field in the {@code Json object} becomes a separate column in the resulting {@link RawRow}.
     *
     * @param dbName The database name to add to the {@code RawRow}.
     * @param tableName The table name to add to the {@code RawRow}.
     * @param rowKey The row key to add to the {@code RawRow}.
     * @param columnsJsonObject The {@code Json object} representing the row columns.
     * @return The resulting {@code RawRow} object.
     * @throws Exception if the input Json does not represent a valid Json object.
     */
    public static RawRow of(String dbName, String tableName, String rowKey, String columnsJsonObject) throws Exception {
        ObjectMapper mapper = JsonUtil.getObjectMapperInstance();
        JsonNode root = mapper.readTree(columnsJsonObject);
        Preconditions.checkState(root.isObject(), "The Json string must represent a Json object.");

        // Parse the Json representation to a Struct
        Struct.Builder columnsBuilder = Struct.newBuilder();
        JsonFormat.parser().merge(columnsJsonObject, columnsBuilder);

        return RawRow.newBuilder()
                .setDbName(dbName)
                .setTableName(tableName)
                .setKey(rowKey)
                .setColumns(columnsBuilder)
                .build();
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
