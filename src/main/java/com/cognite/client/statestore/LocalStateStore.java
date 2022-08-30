package com.cognite.client.statestore;

import com.cognite.client.servicesV1.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;

/**
 * A state store using a local file to persist state entries.
 *
 * {@inheritDoc}
 */
@AutoValue
public abstract class LocalStateStore extends AbstractStateStore {

    private final ObjectReader objectReader = JsonUtil.getObjectMapperInstance().reader();
    private final ObjectWriter objectWriter = JsonUtil.getObjectMapperInstance().writer();

    private static Builder builder() {
        return new AutoValue_LocalStateStore.Builder()
                .setMaxCommitInterval(DEFAULT_MAX_COMMIT_INTERVAL);
    }

    /**
     * Initialize a local state store based on the provided file name.
     *
     * @param fileName the name of the state file.
     * @return the state store.
     * @throws InvalidPathException if the provided file name cannot be resolved to a valid file
     */
    public static LocalStateStore of(String fileName) throws InvalidPathException {
        Preconditions.checkArgument(!Files.isDirectory(Path.of(fileName)),
                "You must specify a valid file name.");
        return LocalStateStore.builder()
                .setPath(Path.of(fileName))
                .build();
    }

    /**
     * Initialize a local state store based on the provided {@link Path}.
     *
     * @param fileName the {@code Path} of the state file.
     * @return the state store.
     * @throws InvalidPathException if the provided {@code Path} name cannot be resolved to a valid file.
     */
    public static LocalStateStore of(Path fileName) throws InvalidPathException {
        Preconditions.checkArgument(!Files.isDirectory(fileName),
                "You must specify a valid file name.");
        return LocalStateStore.builder()
                .setPath(fileName)
                .build();
    }

    abstract Builder toBuilder();

    abstract Path getPath();

    /**
     * Sets the max commit interval.
     *
     * When you activate the commit background thread via {@link #start()}, the state will be committed to
     * persistent storage at least every commit interval.
     *
     * The default max commit interval is 20 seconds.
     * @param interval The target max upload interval.
     * @return The {@link LocalStateStore} with the upload interval configured.
     */
    public LocalStateStore withMaxCommitInterval(Duration interval) {
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

            verifyStateMap();
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
        abstract Builder setPath(Path value);
        abstract Builder setMaxCommitInterval(Duration value);

        abstract LocalStateStore build();
    }
}
