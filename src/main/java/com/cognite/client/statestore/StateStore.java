package com.cognite.client.statestore;

import com.google.protobuf.Struct;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

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
     * Like {@link #setHigh(String, long)}, but only sets the state if the proposed state is higher than the current
     * state.
     *
     * @param key key The id to store the state of.
     * @param value The value of the high watermark.
     */
    public void expandHigh(String key, long value);

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
     * Like {@link #setLow(String, long)}, but only sets the state if the proposed state is lower than the current
     * state.
     *
     * @param key key The id to store the state of.
     * @param value The value of the low watermark.
     */
    public void expandLow(String key, long value);

    /**
     * Get the low watermark state for a single id.
     * @param key The id to get the state of.
     * @return The value of the low watermark. An empty optional in case the state does not exist.
     */
    public OptionalLong getLow(String key);

    /**
     * Check if a state is outside the stored state interval. I.e. if a state is higher than the current high watermark
     * or lower than the current low watermark.
     *
     * The method can be used for determining if a data object should be processed or not.
     *
     * @param key the id to test.
     * @param value the state to test.
     * @return {@code True} if the state is outside of stored state or if the key is previously unseen.
     */
    public boolean isOutsideState(String key, long value);

    /**
     * Get the set of states for a single id. This will give you both the low and high watermark (if set) as a
     * {@link Struct}. The returned {@link Struct} is a read-only view of the state values.
     *
     * @param key The id to get the state of.
     * @return All the states in a Struct.
     */
    public Optional<Struct> getState(String key);

    /**
     * Delete the state(s) for a given id.
     * @param key The id to delete the state(s) of.
     */
    public void deleteState(String key);

    /**
     * Returns a read-only set of all state keys.
     * @return An immutable Set of all state keys.
     */
    public Set<String> keySet();

    /**
     * Start a background thread to perform a commit every {@code maxUploadInterval}. The default upload interval
     * is every 20 seconds.
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
