package com.cognite.client.statestore;

import com.google.protobuf.Struct;
import com.google.protobuf.util.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Abstract parent class for all state store implementations.
 *
 * {@inheritDoc}
 */
public abstract class AbstractStateStore implements StateStore {
    protected static final String COLUMN_KEY_LOW = "low";
    protected static final String COLUMN_KEY_HIGH = "high";
    protected static final Duration DEFAULT_MAX_UPLOAD_INTERVAL = Duration.ofSeconds(30L);

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    protected ConcurrentMap<String, Struct> stateMap = new ConcurrentHashMap<>();
    protected Set<String> modifiedEntries = new ConcurrentSkipListSet<>();

    protected final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    protected ScheduledFuture<?> recurringTask;

    {
        // Make sure the thread pool puts its threads to sleep to allow the JVM to exit without manual
        // clean-up from the client.
        executor.setKeepAliveTime(2000, TimeUnit.SECONDS);
        executor.allowCoreThreadTimeOut(true);
        executor.setRemoveOnCancelPolicy(true);
    }

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

    /*
    Private convenience method to decorate async commit with exception handling.
     */
    private void asyncCommitWrapper() {
        String logPrefix = "asyncCommitWrapper() - ";
        try {
            commit();
        } catch (Exception e) {
            LOG.error(logPrefix + "Exception during commit of the state store {}", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean start() {
        String logPrefix = "start() - ";
        if (null != recurringTask) {
            // The upload task has already been started.
            LOG.warn(logPrefix + "The commit thread has already been started. Start() has no effect.");
            return false;
        }

        recurringTask = executor.scheduleAtFixedRate(this::asyncCommitWrapper,
                1, DEFAULT_MAX_UPLOAD_INTERVAL.getSeconds(), TimeUnit.SECONDS);
        LOG.info(logPrefix + "Starting background thread to commit state at interval {}", DEFAULT_MAX_UPLOAD_INTERVAL);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean stop() {
        String logPrefix = "stop() -";
        if (null == recurringTask) {
            // The upload task has not been started.
            LOG.warn(logPrefix + "The upload thread has not been started. Stop() has no effect.");
            asyncCommitWrapper();
            return false;
        }

        recurringTask.cancel(false);
        boolean returnValue = recurringTask.isDone();
        if (recurringTask.isDone()) {
            // cancellation of task was successful
            recurringTask = null;
            asyncCommitWrapper();
            LOG.info(logPrefix + "Successfully stopped the background commit thread.");
        } else {
            LOG.warn(logPrefix + "Something went wrong when trying to stop the commit thread. Status isDone: {}, "
                            + "isCanceled: {}",
                    recurringTask.isDone(),
                    recurringTask.isCancelled());
        }
        return returnValue;
    }
}
