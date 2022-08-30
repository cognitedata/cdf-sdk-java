package com.cognite.client.statestore;

import com.cognite.client.CogniteClient;
import com.cognite.client.dto.RawRow;
import com.cognite.client.servicesV1.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A state store using a local file to persist state entries.
 *
 * {@inheritDoc}
 */
@AutoValue
public abstract class RawStateStore extends AbstractStateStore {
    private static Builder builder() {
        return new AutoValue_RawStateStore.Builder()
                .setMaxCommitInterval(DEFAULT_MAX_COMMIT_INTERVAL);
    }

    /**
     * Initialize a state store backed by a CDF Raw table.
     *
     * @param client Cognite client to use for the accessing the Raw table
     * @param dbName The name of the Raw data base hosting the backing table
     * @param tableName The name of the Raw table
     * @return the state store.
     */
    public static RawStateStore of(CogniteClient client,
                                   String dbName,
                                   String tableName) {
        return RawStateStore.builder()
                .setClient(client)
                .setDbName(dbName)
                .setTableName(tableName)
                .build();
    }

    abstract Builder toBuilder();

    abstract CogniteClient getClient();
    abstract String getDbName();
    abstract String getTableName();

    /**
     * Sets the max commit interval.
     *
     * When you activate the commit background thread via {@link #start()}, the state will be committed to
     * persistent storage at least every commit interval.
     *
     * The default max commit interval is 20 seconds.
     * @param interval The target max upload interval.
     * @return The {@link RawStateStore} with the upload interval configured.
     */
    public RawStateStore withMaxCommitInterval(Duration interval) {
        Preconditions.checkArgument(interval.compareTo(MAX_MAX_COMMIT_INTERVAL) <= 0
                        && interval.compareTo(MIN_MAX_COMMIT_INTERVAL) >= 0,
                String.format("The max upload interval can be minimum %s and maxmimum %s",
                        MIN_MAX_COMMIT_INTERVAL, MAX_MAX_COMMIT_INTERVAL));
        return toBuilder().setMaxCommitInterval(interval).build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void load() throws Exception {
        String loggingPrefix = "load() - ";

        // Check that the raw db and table exists
        List<String> rawDbNames = new ArrayList<>();
        getClient().raw().databases().list()
                        .forEachRemaining(rawDbNames::addAll);
        if (!rawDbNames.contains(getDbName())) {
            LOG.warn(loggingPrefix + "Raw db {} does not exist. State is not loaded into memory.",
                    getDbName());
            return;
        }
        List<String> rawTableNames = new ArrayList<>();
        getClient().raw().tables().list(getDbName())
                .forEachRemaining(rawTableNames::addAll);
        if (!rawTableNames.contains(getTableName())) {
            LOG.warn(loggingPrefix + "Raw db {} does not contain a table named {}. State is not loaded into memory.",
                    getDbName(),
                    getTableName());
            return;
        }

        // The table exists, let's load it.
        stateMap.clear();
        getClient().raw().rows().list(getDbName(), getTableName())
                .forEachRemaining(rows -> {
                    for (RawRow row : rows) {
                        stateMap.put(row.getKey(), row.getColumns());
                    }
                });
        verifyStateMap();
        LOG.info(loggingPrefix + "Loaded {} state entries from Raw db: {}, table: {}.",
                stateMap.size(),
                getDbName(),
                getTableName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() throws Exception {
        String loggingPrefix = "commit() - ";
        // Since trunc and load can be resource intensive with CDF.Raw, we'll do it incrementally
        // We'll start with processing the deletes
        List<RawRow> deleteRowsList = deletedEntries.stream()
                .map(key -> RawRow.newBuilder()
                        .setDbName(getDbName())
                        .setTableName(getTableName())
                        .setKey(key)
                        .build())
                .collect(Collectors.toList());

        getClient().raw().rows().delete(deleteRowsList);
        deleteRowsList.forEach(row -> deletedEntries.remove(row.getKey()));

        // Then the modified entries
        List<RawRow> modifiedRowsList = modifiedEntries.stream()
                .map(key -> RawRow.newBuilder()
                        .setDbName(getDbName())
                        .setTableName(getTableName())
                        .setKey(key)
                        .setColumns(stateMap.get(key))
                        .build())
                .collect(Collectors.toList());

        getClient().raw().rows().upsert(modifiedRowsList);
        modifiedRowsList.forEach(row -> modifiedEntries.remove(row.getKey()));

        LOG.info(loggingPrefix + "Committed {} deletes and {} modified entries to Raw db: {}, table: {}.",
                deleteRowsList.size(),
                modifiedRowsList.size(),
                getDbName(),
                getTableName());
        if (deletedEntries.size() > 0 || modifiedEntries.size() > 0) {
            LOG.warn(loggingPrefix + "There are some state entries that have not been committed to CDF.Raw. "
                    + "Uncommitted deletes: {}, uncommitted upserts: {}",
                    deletedEntries.size(),
                    modifiedEntries.size());
        }
    }

    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder setClient(CogniteClient value);
        abstract Builder setDbName(String value);
        abstract Builder setTableName(String value);
        abstract Builder setMaxCommitInterval(Duration value);

        abstract RawStateStore build();
    }
}
