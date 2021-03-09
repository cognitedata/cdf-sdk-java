/*
 * Copyright (c) 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.RawRow;
import com.cognite.client.servicesV1.ConnectorConstants;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.RawParser;
import com.google.auto.value.AutoValue;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite Raw rows endpoint.
 *
 * It provides methods for interacting with the Raw row endpoint.
 */
@AutoValue
public abstract class RawRows extends ApiBase {
    private static final int MAX_WRITE_BATCH_SIZE = 1000;
    private static final int MAX_DELETE_BATCH_SIZE = 1000;

    private static Builder builder() {
        return new AutoValue_RawRows.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(RawRows.class);

    /**
     * Constructs a new {@link RawRows} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static RawRows of(CogniteClient client) {
        return RawRows.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all rows from a table.
     *
     * @param dbName the database to list rows from.
     * @param tableName the table to list rows from.
     * @return an {@link Iterator} to page through the rows.
     * @throws Exception
     */
    public Iterator<List<RawRow>> list(String dbName,
                                       String tableName) throws Exception {
        return list(dbName, tableName, Request.create());
    }

    /**
     * Returns all rows from a table. Only the specified columns will be returned for each row.
     *
     * If you provide an empty columns list, only the row keys will be returned.
     *
     * @param dbName the database to list rows from.
     * @param tableName the table to list rows from.
     * @param columns The columns to return.
     * @return an {@link Iterator} to page through the rows.
     * @throws Exception
     */
    public Iterator<List<RawRow>> list(String dbName,
                                       String tableName,
                                       List<String> columns) throws Exception {
        Preconditions.checkNotNull(columns,
                "The columns list cannot be null.");

        // In case there is an empty columns list, we only return the row keys
        String columnsValue = ",";

        if (columns.size() > 0) {
            columnsValue = String.join(",", columns);
        }

        Request request = Request.create()
                .withRootParameter("columns", columnsValue);

        return list(dbName, tableName, request);
    }

    /**
     * Returns all rows from a table. Only the specified columns will be returned for each row.
     *
     * If you provide an empty columns list, only the row keys will be returned.
     *
     * @param dbName the database to list rows from.
     * @param tableName the table to list rows from.
     * @param columns The columns to return.
     * @param requestParameters the column and filter specification for the rows.
     * @return an {@link Iterator} to page through the rows.
     * @throws Exception
     */
    public Iterator<List<RawRow>> list(String dbName,
                                       String tableName,
                                       List<String> columns,
                                       Request requestParameters) throws Exception {
        Preconditions.checkNotNull(columns,
                "The columns list cannot be null.");

        // In case there is an empty columns list, we only return the row keys
        String columnsValue = ",";

        if (columns.size() > 0) {
            columnsValue = String.join(",", columns);
        }

        Request request = requestParameters
                .withRootParameter("columns", columnsValue);

        return list(dbName, tableName, request);
    }

    /**
     * Returns a set of rows from a table.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The rows are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param dbName the database to list rows from.
     * @param tableName the table to list rows from.
     * @param requestParameters the column and filter specification for the rows.
     * @return an {@link Iterator} to page through the rows.
     * @throws Exception
     */
    public Iterator<List<RawRow>> list(String dbName,
                                       String tableName,
                                       Request requestParameters) throws Exception {
        Preconditions.checkArgument(dbName != null && !dbName.isEmpty(),
                "You must specify a data base name.");
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(),
                "You must specify a table name.");

        // Get the cursors for parallel retrieval
        int noCursors = getClient().getClientConfig().getNoListPartitions();
        List<String> cursors = retrieveCursors(dbName, tableName, noCursors, requestParameters);

        //return FanOutIterator.of(ImmutableList.of(futureIterator));
        return list(dbName, tableName, requestParameters, cursors.toArray(new String[cursors.size()]));
    }

    /**
     * Returns a set of rows from a table.
     * The rows are returned for the specified partitions. This is method is intended for advanced use
     * cases where you need direct control over
     * the individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param dbName the database to list rows from.
     * @param tableName the table to list rows from.
     * @param requestParameters the column and filter specification for the rows.
     * @return an {@link Iterator} to page through the rows.
     * @throws Exception
     */
    public Iterator<List<RawRow>> list(String dbName,
                                       String tableName,
                                       Request requestParameters,
                                       String... cursors) throws Exception {
        Preconditions.checkArgument(dbName != null && !dbName.isEmpty(),
                "You must specify a data base name.");
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(),
                "You must specify a table name.");

        // Add db name, table name and default limit
        Request request = requestParameters
                .withRootParameter("dbName", dbName)
                .withRootParameter("tableName", tableName)
                .withRootParameter("limit", requestParameters.getRequestParameters()
                        .getOrDefault("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE_RAW));


        return AdapterIterator.of(listJson(ResourceType.RAW_ROW, request, "cursor", cursors),
                RawRowParser.of(dbName, tableName));
    }

    /**
     * Retrieves a set of rows based on row key.
     *
     * @param dbName The database to read rows from.
     * @param tableName The table to read rows from.
     * @param rowKeys The set of row keys to retrieve.
     * @return The rows from Raw
     * @throws Exception
     */
    public List<RawRow> retrieve(String dbName,
                                 String tableName,
                                 Collection<String> rowKeys) throws Exception {
        String loggingPrefix = "retrieve() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Instant startInstant = Instant.now();
        Preconditions.checkArgument(null!= dbName && !dbName.isEmpty(),
                "Database cannot be null or empty.");
        Preconditions.checkArgument(null!= tableName && !tableName.isEmpty(),
                "Table name cannot be null or empty.");
        Preconditions.checkNotNull(rowKeys,
                "Row keys cannot be null.");
        LOG.info(loggingPrefix + "Received {} rows to retrieve.",
                rowKeys.size());

        ConnectorServiceV1 connector = getClient().getConnectorService();
        ItemReader<String> rowReader = connector.readRawRow();
        List<String> completedBatches = new ArrayList<>();

        // Submit all batches
        Map<CompletableFuture<ResponseItems<String>>, String> responseMap = new HashMap<>();
        for (String rowKey : rowKeys) {
            Request request = Request.create()
                    .withRootParameter("dbName", dbName)
                    .withRootParameter("tableName", tableName)
                    .withRootParameter("rowKey", rowKey);

            responseMap.put(rowReader.getItemsAsync(addAuthInfo(request)), rowKey);
        }

        // Wait for all requests futures to complete
        List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
        responseMap.keySet().forEach(future -> futureList.add(future));
        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        allFutures.join(); // Wait for all futures to complete

        // Collect the responses from the futures
        Map<ResponseItems<String>, String> resultsMap = new HashMap<>(responseMap.size());
        for (Map.Entry<CompletableFuture<ResponseItems<String>>, String> entry : responseMap.entrySet()) {
            resultsMap.put(entry.getKey().join(), entry.getValue());
        }

        // Check the responses
        for (ResponseItems<String> response : resultsMap.keySet()) {
            if (response.isSuccessful()) {
                completedBatches.addAll(response.getResultsItems());
                LOG.debug(loggingPrefix + "Retrieve row request success. Adding row to result collection.");
            } else {
                LOG.error(loggingPrefix + "Retrieve row request failed: {}", response.getResponseBodyAsString());
                throw new Exception(String.format(loggingPrefix + "Retrieve row request failed: %s",
                        response.getResponseBodyAsString()));
            }
        }

        LOG.info(loggingPrefix + "Successfully retrieve {} rows within a duration of {}.",
                rowKeys.size(),
                Duration.between(startInstant, Instant.now()).toString());

        RawRowParser parser = RawRowParser.of(dbName, tableName);

        return completedBatches.stream()
                .map(json -> parser.apply(json))
                .collect(Collectors.toList());
    }

    /**
     * Retrieves cursors for parallel retrieval of rows from Raw.
     *
     * This is intended for advanced use cases where you need granular control of the parallel retrieval from
     * Raw--for example in distributed processing frameworks. Most scenarios should just use
     * {@code list} directly as that will automatically handle parallelization for you.
     *
     * @param dbName The database to retrieve row cursors from.
     * @param tableName The table to retrieve row cursors from.
     * @param requestParameters Hosts query parameters like max and min time stamps and number of cursors to request.
     * @return A list of cursors.
     * @throws Exception
     */
    public List<String> retrieveCursors(String dbName,
                                        String tableName,
                                        Request requestParameters) throws Exception {
        return retrieveCursors(dbName,
                tableName,
                getClient().getClientConfig().getNoListPartitions(),
                requestParameters);
    }

    /**
     * Retrieves cursors for parallel retrieval of rows from Raw.
     *
     * This is intended for advanced use cases where you need granular control of the parallel retrieval from
     * Raw--for example in distributed processing frameworks. Most scenarios should just use
     * {@code list} directly as that will automatically handle parallelization for you.
     *
     * @param dbName The database to retrieve row cursors from.
     * @param tableName The table to retrieve row cursors from.
     * @param noCursors The number of cursors.
     * @param requestParameters Hosts query parameters like max and min time stamps and number of cursors to request.
     * @return A list of cursors.
     * @throws Exception
     */
    public List<String> retrieveCursors(String dbName,
                                        String tableName,
                                        int noCursors,
                                        Request requestParameters) throws Exception {
        String loggingPrefix = "retrieveCursors() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Instant startInstant = Instant.now();
        Preconditions.checkArgument(dbName != null && !dbName.isEmpty(),
                "You must specify a data base name.");
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(),
                "You must specify a table name.");

        // Build request
        Request request = requestParameters
                .withRootParameter("dbName", dbName)
                .withRootParameter("tableName", tableName)
                .withRootParameter("numberOfCursors", noCursors);

        ConnectorServiceV1 connector = getClient().getConnectorService();
        ItemReader<String> cursorItemReader = connector.readCursorsRawRows();
        List<String> results = cursorItemReader
                .getItems(addAuthInfo(request))
                .getResultsItems();

        LOG.info(loggingPrefix + "Retrieved {} cursors. Duration: {}",
                results.size(),
                Duration.between(startInstant, Instant.now()));

        return results;
    }

    /**
     * Creates rows in raw tables.
     *
     * @param rows The rows to upsert.
     * @param ensureParent Set to true to create the row tables if they don't already exist.
     * @return The created table names.
     * @throws Exception
     */
    public List<RawRow> upsert(List<RawRow> rows, boolean ensureParent) throws Exception {
        String loggingPrefix = "upsert() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Instant startInstant = Instant.now();
        Preconditions.checkArgument(null!= rows,
                "Rows list cannot be empty.");
        LOG.info(loggingPrefix + "Received {} rows to upsert.",
                rows.size());

        final int maxUpsertLoopIterations = 2;
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter upsertWriter = connector.writeRawRows();

        List<List<RawRow>> upsertBatches = groupAndBatch(rows, MAX_WRITE_BATCH_SIZE);
        List<List<RawRow>> completedBatches = new ArrayList<>();

        ThreadLocalRandom random = ThreadLocalRandom.current();
        String exceptionMessage = "";
        for (int i = 0; i < maxUpsertLoopIterations && upsertBatches.size() > 0;
             i++, Thread.sleep(Math.min(500L, (10L * (long) Math.exp(i)) + random.nextLong(5)))) {
            LOG.debug(loggingPrefix + "Start upsert loop {} with {} batches to upsert and "
                            + "{} completed batches at duration {}",
                    i,
                    upsertBatches.size(),
                    completedBatches.size(),
                    Duration.between(startInstant, Instant.now()).toString());

            // Submit all batches
            Map<CompletableFuture<ResponseItems<String>>, List<RawRow>> responseMap = new HashMap<>();
            for (List<RawRow> batch : upsertBatches) {
                List<Map<String, Object>> upsertItems = new ArrayList<>();
                batch.stream()
                        .forEach(row -> upsertItems.add(toRequestInsertItem(row)));

                Request request = Request.create()
                        .withItems(upsertItems)
                        .withRootParameter("ensureParent", ensureParent)
                        .withRootParameter("dbName", batch.get(0).getDbName())
                        .withRootParameter("tableName", batch.get(0).getTableName());

                responseMap.put(upsertWriter.writeItemsAsync(addAuthInfo(request)), batch);
            }
            upsertBatches = new ArrayList<>();

            // Wait for all requests futures to complete
            List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
            responseMap.keySet().forEach(future -> futureList.add(future));
            CompletableFuture<Void> allFutures =
                    CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
            allFutures.join(); // Wait for all futures to complete

            // Collect the responses from the futures
            Map<ResponseItems<String>, List<RawRow>> resultsMap = new HashMap<>(responseMap.size());
            for (Map.Entry<CompletableFuture<ResponseItems<String>>, List<RawRow>> entry : responseMap.entrySet()) {
                resultsMap.put(entry.getKey().join(), entry.getValue());
            }

            // Check the responses
            for (ResponseItems<String> response : resultsMap.keySet()) {
                if (response.isSuccessful()) {
                    completedBatches.add(resultsMap.get(response));
                    LOG.debug(loggingPrefix + "Upsert items request success. Adding batch to result collection.");
                } else {
                    exceptionMessage = response.getResponseBodyAsString();
                    LOG.debug(loggingPrefix + "Upsert items request failed: {}", response.getResponseBodyAsString());
                    if (i == maxUpsertLoopIterations - 1) {
                        // Add the error message to std logging
                        LOG.error(loggingPrefix + "Upsert items request failed. {}", response.getResponseBodyAsString());
                    }

                    // Add the failed batch to be re-tried
                    upsertBatches.add(resultsMap.get(response));
                }
            }
        }

        // Check if all elements completed the upsert requests
        if (upsertBatches.isEmpty()) {
            LOG.info(loggingPrefix + "Successfully upserted {} rows within a duration of {}.",
                    rows.size(),
                    Duration.between(startInstant, Instant.now()).toString());
        } else {
            LOG.error(loggingPrefix + "Failed to upsert rows. {} batches remaining. {} batches completed upsert."
                            + System.lineSeparator() + "{}",
                    upsertBatches.size(),
                    completedBatches.size(),
                    exceptionMessage);
            throw new Exception(String.format(loggingPrefix + "Failed to upsert rows. %d batches remaining. "
                            + " %d batches completed upsert. %n " + exceptionMessage,
                    upsertBatches.size(),
                    completedBatches.size()));
        }

        return rows;
    }

    /**
     * Creates rows in raw tables.
     *
     * If the row tables don't exist from before, they will also be created.
     *
     * @param rows The rows to upsert.
     * @return The created table names.
     * @throws Exception
     */
    public List<RawRow> upsert(List<RawRow> rows) throws Exception {
        return upsert(rows, true);
    }

    /**
     * Deletes a set of rows from Raw tables.
     *
     * @param rows The row keys to delete.
     * @return The deleted rows
     * @throws Exception
     */
    public List<RawRow> delete(Collection<RawRow> rows) throws Exception {
        String loggingPrefix = "delete() - ";
        Instant startInstant = Instant.now();
        Preconditions.checkArgument(null!= rows,
                "Rows list cannot be empty.");
        LOG.info(loggingPrefix + "Received {} rows to delete.",
                rows.size());

        final int maxDeleteLoopIterations = 2;
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteWriter = connector.deleteRawRows();

        List<List<RawRow>> deleteBatches = groupAndBatch(rows, MAX_DELETE_BATCH_SIZE);
        List<List<RawRow>> completedBatches = new ArrayList<>();

        ThreadLocalRandom random = ThreadLocalRandom.current();
        String exceptionMessage = "";
        for (int i = 0; i < maxDeleteLoopIterations && deleteBatches.size() > 0;
             i++, Thread.sleep(Math.min(500L, (10L * (long) Math.exp(i)) + random.nextLong(5)))) {
            LOG.debug(loggingPrefix + "Start upsert loop {} with {} batches to upsert and "
                            + "{} completed batches at duration {}",
                    i,
                    deleteBatches.size(),
                    completedBatches.size(),
                    Duration.between(startInstant, Instant.now()).toString());

            // Submit all batches
            Map<CompletableFuture<ResponseItems<String>>, List<RawRow>> responseMap = new HashMap<>();
            for (List<RawRow> batch : deleteBatches) {
                List<Map<String, Object>> deleteItems = new ArrayList<>();
                batch.stream()
                        .forEach(row -> deleteItems.add(ImmutableMap.of("key", row.getKey())));

                Request request = Request.create()
                        .withItems(deleteItems)
                        .withRootParameter("dbName", batch.get(0).getDbName())
                        .withRootParameter("tableName", batch.get(0).getTableName());

                responseMap.put(deleteWriter.writeItemsAsync(addAuthInfo(request)), batch);
            }
            deleteBatches = new ArrayList<>();

            // Wait for all requests futures to complete
            List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
            responseMap.keySet().forEach(future -> futureList.add(future));
            CompletableFuture<Void> allFutures =
                    CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
            allFutures.join(); // Wait for all futures to complete

            // Collect the responses from the futures
            Map<ResponseItems<String>, List<RawRow>> resultsMap = new HashMap<>(responseMap.size());
            for (Map.Entry<CompletableFuture<ResponseItems<String>>, List<RawRow>> entry : responseMap.entrySet()) {
                resultsMap.put(entry.getKey().join(), entry.getValue());
            }

            // Check the responses
            for (ResponseItems<String> response : resultsMap.keySet()) {
                if (response.isSuccessful()) {
                    completedBatches.add(resultsMap.get(response));
                    LOG.debug(loggingPrefix + "Delete items request success. Adding batch to result collection.");
                } else {
                    exceptionMessage = response.getResponseBodyAsString();
                    LOG.debug(loggingPrefix + "Delete items request failed: {}", response.getResponseBodyAsString());
                    if (i == maxDeleteLoopIterations - 1) {
                        // Add the error message to std logging
                        LOG.error(loggingPrefix + "Delete items request failed. {}", response.getResponseBodyAsString());
                    }

                    // Add the failed batch to be re-tried
                    deleteBatches.add(resultsMap.get(response));
                }
            }
        }

        // Check if all elements completed the delete requests
        if (deleteBatches.isEmpty()) {
            LOG.info(loggingPrefix + "Successfully deleted {} rows within a duration of {}.",
                    rows.size(),
                    Duration.between(startInstant, Instant.now()).toString());
        } else {
            LOG.error(loggingPrefix + "Failed to delete rows. {} batches remaining. {} batches completed."
                            + System.lineSeparator() + "{}",
                    deleteBatches.size(),
                    completedBatches.size(),
                    exceptionMessage);
            throw new Exception(String.format(loggingPrefix + "Failed to delete rows. %d batches remaining. "
                            + " %d batches completed delete. %n " + exceptionMessage,
                    deleteBatches.size(),
                    completedBatches.size()));
        }

        return new ArrayList(rows);
    }

    /**
     * Group and batch {@link RawRow}.
     *
     * The rows will be de-duplicated and grouped by database and table. This aligns well with upsert and delete operations
     * where each API request must target a specific db and table.
     *
     * @param rows The rows to group and batch
     * @param maxBatchSize The max batch size of the output.
     * @return A list of batches with rows.
     */
    private List<List<RawRow>> groupAndBatch(Collection<RawRow> rows, int maxBatchSize) {
        String loggingPrefix = "groupAndBatch() - ";
        Instant startInstant = Instant.now();
        LOG.debug(loggingPrefix + "Received {} rows to group and batch.",
                rows.size());

        Collection<RawRow> deduplicated = deduplicate(rows);

        // Group by db and table
        Map<String, List<RawRow>> tableMap = new HashMap<>();
        for (RawRow row : deduplicated) {
            String key = row.getDbName() + row.getTableName();
            List<RawRow> group = tableMap.getOrDefault(key, new ArrayList<RawRow>());
            group.add(row);
            tableMap.put(key, group);
        }

        // Split into batches
        List<List<RawRow>> allBatches = new ArrayList<>();
        for (List<RawRow> group : tableMap.values()) {
            List<RawRow> batch = new ArrayList<>();
            for (RawRow row : group) {
                batch.add(row);
                if (batch.size() >= maxBatchSize) {
                    allBatches.add(batch);
                    batch = new ArrayList<>();
                }
            }
            if (batch.size() > 0) {
                allBatches.add(batch);
            }
        }

        LOG.debug(loggingPrefix + "Finished grouping {} rows into {} batches. Duration: {}",
                rows.size(),
                allBatches.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return allBatches;
    }

    /**
     * Deduplicates a collection of {@link RawRow}.
     *
     * The rows are deduplicated based on a natural key of dbName, tableName and row key.
     *
     * @param rows The rows to deduplicate.
     * @return The deduplicated rows.
     */
    private Collection<RawRow> deduplicate(Collection<RawRow> rows) {
        String loggingPrefix = "deduplicate() - ";
        Instant startInstant = Instant.now();
        LOG.debug(loggingPrefix + "Received {} rows to deduplicate.",
                rows.size());

        // Group by db, table and row key
        Map<String, RawRow> keyMap = new HashMap<>();
        for (RawRow row : rows) {
            keyMap.put(row.getDbName() + row.getTableName() + row.getKey(), row);
        }

        LOG.debug(loggingPrefix + "Finished deduplicating {} input rows into {} resulting rows. Duration: {}",
                rows.size(),
                keyMap.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return keyMap.values();
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(RawRow item) {
        try {
            return RawParser.toRequestInsertItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Helper class to parse raw rows from Json representation to protobuf.
     */
    @AutoValue
    abstract static class RawRowParser implements Function<String, RawRow> {

        private static Builder builder() {
            return new AutoValue_RawRows_RawRowParser.Builder();
        }

        public static RawRowParser of(String dbName, String tableName) {
            return RawRowParser.builder()
                    .setDbName(dbName)
                    .setTableName(tableName)
                    .build();
        }

        abstract String getDbName();
        abstract String getTableName();

        @Override
        public RawRow apply(String json) {
            try {
                return RawParser.parseRawRow(getDbName(), getTableName(), json);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setDbName(String value);
            abstract Builder setTableName(String value);

            abstract RawRowParser build();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract RawRows build();
    }
}
