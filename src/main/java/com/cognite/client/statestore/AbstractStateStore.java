package com.cognite.client.statestore;

import com.google.protobuf.Struct;
import com.google.protobuf.util.Values;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Abstract parent class for all state store implementations.
 *
 * {@inheritDoc}
 */
public abstract class AbstractStateStore implements StateStore {

    protected final String COLUMN_KEY_LOW = "low";
    protected final String COLUMN_KEY_HIGH = "high";
    protected ConcurrentMap<String, Struct> stateMap = new ConcurrentHashMap<>();
    protected Set<String> modifiedEntries = new ConcurrentSkipListSet<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLow(String key, long value) {
        Struct existingEntry = stateMap.getOrDefault(key, Struct.getDefaultInstance());
        Struct newEntry = existingEntry.toBuilder()
                .putFields(COLUMN_KEY_LOW, Values.of(value))
                .build();
        stateMap.put(key, newEntry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OptionalLong getLow(String key) {
        if (null == stateMap.get(key) || null == stateMap.get(key).getFieldsMap().get(COLUMN_KEY_LOW)) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(Math.round(stateMap.get(key).getFieldsMap().get(COLUMN_KEY_LOW).getNumberValue()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setHigh(String key, long value){
        Struct existingEntry = stateMap.getOrDefault(key, Struct.getDefaultInstance());
        Struct newEntry = existingEntry.toBuilder()
                .putFields(COLUMN_KEY_HIGH, Values.of(value))
                .build();
        stateMap.put(key, newEntry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OptionalLong getHigh(String key) {
        if (null == stateMap.get(key) || null == stateMap.get(key).getFieldsMap().get(COLUMN_KEY_HIGH)) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(Math.round(stateMap.get(key).getFieldsMap().get(COLUMN_KEY_HIGH).getNumberValue()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Struct> getState(String key) {
        return Optional.ofNullable(stateMap.get(key));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteState(String key) {
        stateMap.remove(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void load() throws Exception;

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void commit() throws Exception;

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract boolean start();

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract boolean stop();
}
