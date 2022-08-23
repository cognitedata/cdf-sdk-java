package com.cognite.client.statestore;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * The {@code StateStore} helps keep track of the extraction/processing state of a data application (extractor,
 * data pipeline, contextualization pipeline, etc.). It is designed to keep track of watermarks to enable incremental
 * load patterns.
 *
 * At the beginning of a run the data application typically calls the {@code load()} method, which loads the states
 * from the remote store (which can either be a local JSON file or a table in CDF RAW), and during and/or
 * at the end of a run, the commit method is called, which saves the current states to the persistent store.
 */
public interface StateStore {

    /**
     * Load the states from the persistent store.
     * @throws Exception
     */
    public void load() throws Exception;

    /**
     * Commit the current states to the persistent store. Will overwrite/replace previously persisted states.
     * @throws Exception
     */
    public void commit() throws Exception;

    /**
     * Set/update the high watermark for a single id.
     * @param key The id to store the state of.
     * @param value The value of the high watermark.
     */
    public void setHigh(String key, long value);

    /**
     * Get the high watermark state for a single id.
     * @param key The id to get the state of.
     * @return The value of the high watermark. An empty optional in case the state does not exist.
     */
    public OptionalLong getHigh(String key);

    /**
     * Set/update the low watermark for a single id.
     * @param key The id to store the state of.
     * @param value The value of the low watermark.
     */
    public void setLow(String key, long value);

    /**
     * Get the low watermark state for a single id.
     * @param key The id to get the state of.
     * @return The value of the low watermark. An empty optional in case the state does not exist.
     */
    public OptionalLong getLow(String key);

    /**
     * Get the set of states for a single id.
     *
     * @param key The id to get the state of.
     * @return All the states in a Map.
     */
    public Optional<Map<String, Object>> getState(String key);

    /**
     * Delete the state(s) for a given id.
     * @param key The id to delete the state(s) of.
     */
    public void deleteState(String key);

    /**
     * Start a background thread to perform a commit every {@code maxUploadInterval}. The default upload interval
     * is every 30 seconds.
     *
     * If the background thread has already been started (for example by an earlier call to {@code start()} then this
     * method does nothing and returns {@code false}.
     *
     * @return {@code true} if the upload thread started successfully, {@code false} if the background thread has already
     * been started.
     */
    public boolean start();

    /**
     * Stops the background thread if it is running and ensures the upload queue is empty by calling {@code upload()} one
     * last time after shutting down the thread.
     *
     * @return {@code true} if the upload thread stopped successfully, {@code false} if the upload thread was not started
     * in the first place.
     */
    public boolean stop();
}
