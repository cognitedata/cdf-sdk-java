package com.cognite.client.statestore;

import com.cognite.client.CogniteClient;
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

/**
 * A state store using a local file to persist state entries.
 *
 * {@inheritDoc}
 */
@AutoValue
public abstract class RawStateStore extends AbstractStateStore {

    private final ObjectReader objectReader = JsonUtil.getObjectMapperInstance().reader();
    private final ObjectWriter objectWriter = JsonUtil.getObjectMapperInstance().writer();

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

        getClient().raw().rows().list(getDbName(), getTableName());
        if (Files.exists(getPath())) {
            stateMap.clear();
            JsonNode root = objectReader.readTree(Files.readAllBytes(getPath()));
            Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                Struct.Builder structBuilder = Struct.newBuilder();
                JsonFormat.parser().merge(entry.getValue().toString(), structBuilder);
                stateMap.put(entry.getKey(), structBuilder.build());
            }

            //stateMap = objectReader.readValue(getPath().toFile(), stateMap.getClass());
            LOG.info(loggingPrefix + "Loaded {} state entries from {}.", stateMap.size(), getPath().toString());
        } else {
            LOG.info(loggingPrefix + "File {} not found. No persisted state loaded into memory.",
                    getPath().toString());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() throws Exception {
        objectWriter.writeValue(getPath().toFile(), stateMap);
        LOG.info("commit() - Committed {} state entries to {}.", stateMap.size(), getPath().toString());
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
