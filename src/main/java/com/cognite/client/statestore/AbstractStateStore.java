package com.cognite.client.statestore;

import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Abstract parent class for all state store implementations.
 *
 *
 */
public abstract class AbstractStateStore implements StateStore {

    protected final String COLUMN_KEY_LOW = "low";
    protected final String COLUMN_KEY_HIGH = "high";
    protected ConcurrentMap<String, Map<String,Object>> stateMap = new ConcurrentHashMap<>();
    protected Set<String> modifiedEntries = new ConcurrentSkipListSet<>();

    public OptionalLong getLow(String key) {
        if (null == stateMap.get(key) || null == stateMap.get(key).get(COLUMN_KEY_LOW)) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of((long) stateMap.get(key).get(COLUMN_KEY_LOW));
        }
    }

    public OptionalLong getHigh(String key) {
        if (null == stateMap.get(key) || null == stateMap.get(key).get(COLUMN_KEY_HIGH)) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of((long) stateMap.get(key).get(COLUMN_KEY_HIGH));
        }
    }

    public abstract void load() throws Exception;

    public abstract void commit() throws Exception;
}
