package com.cognite.client.statestore;

import java.util.OptionalLong;

public interface StateStore {

    public void load() throws Exception;

    public void commit() throws Exception;

    public OptionalLong getHigh(String key);

    public OptionalLong getLow(String key);
}
