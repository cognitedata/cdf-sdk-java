package com.cognite.client.statestore;

import com.cognite.client.servicesV1.util.JsonUtil;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;

import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

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
        if (Files.exists(getPath())) {
            stateMap = objectReader.readValue(getPath().toFile(), stateMap.getClass());
            LOG.info("load() - Loaded {} state entries from {}.", stateMap.size(), getPath().toString());
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
