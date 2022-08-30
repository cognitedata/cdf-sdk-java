package com.cognite.client.statestore;

import com.cognite.client.util.ParseValue;
import com.google.protobuf.Struct;
import com.google.protobuf.util.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Abstract parent class for all state store implementations.
 *
 * {@inheritDoc}
 */
public abstract class AbstractStateStore implements StateStore {
    protected static final String COLUMN_KEY_LOW = "low";
    protected static final String COLUMN_KEY_HIGH = "high";
    protected static final Duration MIN_MAX_COMMIT_INTERVAL = Duration.ofSeconds(1L);
    protected static final Duration DEFAULT_MAX_COMMIT_INTERVAL = Duration.ofSeconds(20L);
    protected static final Duration MAX_MAX_COMMIT_INTERVAL = Duration.ofMinutes(60L);

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    protected ConcurrentMap<String, Struct> stateMap = new ConcurrentHashMap<>();
    protected Set<String> modifiedEntries = new ConcurrentSkipListSet<>();
    protected Set<String> deletedEntries = new ConcurrentSkipListSet<>();

    protected final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    protected ScheduledFuture<?> recurringTask;

    {
        // Make sure the thread pool puts its threads to sleep to allow the JVM to exit without manual
        // clean-up from the client.
        executor.setKeepAliveTime(2000, TimeUnit.SECONDS);
        executor.allowCoreThreadTimeOut(true);
        executor.setRemoveOnCancelPolicy(true);
    }

    abstract Duration getMaxCommitInterval();

    /**
     * {@inheritDoc}
     */
    @Override
    public void setHigh(String key, long value){
        Struct existingEntry = stateMap.getOrDefault(key, Struct.getDefaultInstance());
        // We add the long as a string to ensure full precision of the long. If we store it as a number
        // we'll be limited to 53 bit precision due to json's double numeric type.
        Struct newEntry = existingEntry.toBuilder()
                .putFields(COLUMN_KEY_HIGH, Values.of(String.valueOf(value)))
                .build();
        stateMap.put(key, newEntry);
        modifiedEntries.add(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void expandHigh(String key, long value) {
        getHigh(key).ifPresentOrElse(
                currentValue -> {
                    if (value > currentValue) {
                        setHigh(key, value);
                    }
                },
                () -> setHigh(key, value)
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OptionalLong getHigh(String key) {
        if (null == stateMap.get(key) || null == stateMap.get(key).getFieldsMap().get(COLUMN_KEY_HIGH)) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(ParseValue.parseLong(stateMap.get(key).getFieldsMap().get(COLUMN_KEY_HIGH)));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLow(String key, long value) {
        Struct existingEntry = stateMap.getOrDefault(key, Struct.getDefaultInstance());
        // We add the long as a string to ensure full precision of the long. If we store it as a number
        // we'll be limited to 53 bit precision due to json's double numeric type.
        Struct newEntry = existingEntry.toBuilder()
                .putFields(COLUMN_KEY_LOW, Values.of(String.valueOf(value)))
                .build();
        stateMap.put(key, newEntry);
        modifiedEntries.add(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void expandLow(String key, long value) {
        getLow(key).ifPresentOrElse(
                currentValue -> {
                    if (value < currentValue) {
                        setLow(key, value);
                    }
                },
                () -> setLow(key, value)
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OptionalLong getLow(String key) {
        if (null == stateMap.get(key) || null == stateMap.get(key).getFieldsMap().get(COLUMN_KEY_LOW)) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(ParseValue.parseLong(stateMap.get(key).getFieldsMap().get(COLUMN_KEY_LOW)));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOutsideState(String key, long value) {
        if (!stateMap.containsKey(key)) {
            // the key has not been seen before
            return true;
        }
        if (getHigh(key).isPresent() && getHigh(key).getAsLong() < value) {
            return true;
        }
        if (getLow(key).isPresent() && getLow(key).getAsLong() > value) {
            return true;
        }

        return false;
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
        deletedEntries.add(key);
    }

    /**
     * {@inheritDoc}
     */
    public Set<String> keySet() {
        return Set.copyOf(stateMap.keySet());
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

    /*
    Check that the state map is valid
     */
    protected boolean verifyStateMap() {
        List<Map.Entry<String, Struct>> invalidEntries = stateMap.entrySet().stream()
                .filter(entry -> !entry.getValue().containsFields(COLUMN_KEY_HIGH) && !entry.getValue().containsFields(COLUMN_KEY_LOW))
                .collect(Collectors.toList());

        if (invalidEntries.isEmpty()) {
            return true;
        } else {
            LOG.warn("verifyStateMap() - Found {} invalid entries in the state store. Some of the invalid entries "
            + "include {}",
                    invalidEntries.size(),
                    invalidEntries.stream().map(entry -> entry.getKey()).limit(10).collect(Collectors.toList()));
            return false;
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
                1, getMaxCommitInterval().getSeconds(), TimeUnit.SECONDS);
        LOG.info(logPrefix + "Starting background thread to commit state at interval {}", getMaxCommitInterval());
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
