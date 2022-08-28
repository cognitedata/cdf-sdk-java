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
        return new AutoValue_LocalStateStore.Builder();
    }

    public static LocalStateStore of(String fileName) throws InvalidPathException {
        Preconditions.checkArgument(!Files.isDirectory(Path.of(fileName)),
                "You must specify a valid file name.");
        return LocalStateStore.builder()
                .setPath(Path.of(fileName))
                .build();
    }

    public static LocalStateStore of(Path fileName) throws InvalidPathException {
        Preconditions.checkArgument(!Files.isDirectory(fileName),
                "You must specify a valid file name.");
        return LocalStateStore.builder()
                .setPath(fileName)
                .build();
    }

    //abstract Builder toBuilder();

    abstract Path getPath();

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
        abstract Builder setPath(Path value);

        abstract LocalStateStore build();
    }
}
