package com.cognite.client.stream;

import com.cognite.client.RawRows;
import com.cognite.client.Request;
import com.cognite.client.dto.RawRow;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 *
 */
@AutoValue
public abstract class RawPublisher {
    protected static final Logger LOG = LoggerFactory.getLogger(RawPublisher.class);

    private AtomicBoolean abortStream = new AtomicBoolean(false);
    private State state = State.READY;

    private static Builder builder() {
        return new AutoValue_RawPublisher.Builder()
                .setPollingInterval(Duration.ofSeconds(5))
                .setPollingOffset(Duration.ofSeconds(2))
                .setStartTime(Instant.ofEpochMilli(1L))
                .setEndTime(Instant.MAX)
                ;
    }

    public static RawPublisher of(RawRows rawRows,
                                  String rawDbName,
                                  String rawTableName) {
        return RawPublisher.builder()
                .setRawRows(rawRows)
                .setRawDbName(rawDbName)
                .setRawTableName(rawTableName)
                .build();
    }

    abstract Builder toBuilder();

    abstract RawRows getRawRows();
    abstract String getRawDbName();
    abstract String getRawTableName();
    abstract Duration getPollingInterval();
    abstract Duration getPollingOffset();
    abstract Instant getStartTime();
    abstract Instant getEndTime();
    @Nullable
    abstract Consumer<List<RawRow>> getConsumer();

    /**
     * Add the consumer of the data stream.
     *
     * The consumer will be called for each batch of {@link RawRow}. This is potentially a blocking operation,
     * so you should take care to process the batch efficiently (or spin off processing to a separate thread).
     *
     * @param consumer The function to call for each batch of {@link RawRow}.
     * @return A {@link RawPublisher} with the consumer configured.
     */
    public RawPublisher withConsumer(Consumer<List<RawRow>> consumer) {
        return toBuilder().setConsumer(consumer).build();
    }

    public RawPublisher withEndTime(Instant endTime) {
        return toBuilder().setEndTime(endTime).build();
    }

    public RawPublisher withPollingInterval(Duration interval) {
        return toBuilder().setPollingInterval(interval).build();
    }

    /**
     * Starts the streaming job.
     *
     * The job is executed on a separate thread and this method will return immediately to the caller. It returns
     * a {@link Future} that you can use to block the execution of your own code if you want to explicitly
     * wait for completion of the streaming job.
     *
     * @return A Future hosting the end state of the streaming job. The future returns {@code true} when the
     * polling loop completes (at its specified end time). {@code false} if the job is aborted before the specified end time.
     */
    public Future<Boolean> start() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Boolean> future = executorService.submit(this::run);
        executorService.shutdown();
        return future;
    }

    /**
     * Aborts the current stream operation. It may take a few seconds for this operation to complete.
     */
    public void abort() {
        abortStream.set(true);
        while (State.RUNNING == state) {
            // wait for the stream to close
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                break;
            }
        }
    }

    /**
     * Start the main polling loop for reading rows from a raw table.
     *
     * @return {@code true} when the polling loop completes (at the specified end time). {@code false} if the
     * job is aborted before the specified end time.
     * @throws Exception
     */
    boolean run() throws Exception {
        final String loggingPrefix = "streaming() [" + RandomStringUtils.randomAlphanumeric(6) + "] - ";
        Preconditions.checkNotNull(getConsumer(),
                loggingPrefix + "You must specify a Consumer via withConsumer(Consumer<List<RawRow>>)");
        Preconditions.checkState(getStartTime().isBefore(getEndTime()),
                String.format(loggingPrefix + "Start time must be before end time. Start time: %s. End time: %s",
                        getStartTime(),
                        getEndTime()));
        LOG.info(loggingPrefix + "Setting up streaming read from CDF.Raw: {}.{}",
                getRawDbName(),
                getRawTableName());
        state = State.RUNNING;

        // Set the time range for the first query
        long startRange = getStartTime().toEpochMilli();
        long endRange = Instant.now().minus(getPollingOffset()).toEpochMilli();

        while (Instant.now().isBefore(getEndTime().plus(getPollingOffset())) && !abortStream.get()) {
            endRange = Instant.now().minus(getPollingOffset()).toEpochMilli();
            LOG.debug(loggingPrefix + "Enter polling loop with startRange: [{}] and endRange: [{}]",
                    startRange,
                    endRange);
            if (startRange < endRange) {
                Request query = Request.create()
                        .withRootParameter("minLastUpdatedTime", startRange)
                        .withRootParameter("maxLastUpdatedTime", endRange);
                LOG.debug(loggingPrefix + "Send request to read CDF Raw: {}",
                        query);

                Iterator<List<RawRow>> iterator = getRawRows().list(getRawDbName(), getRawTableName(), query);
                while (iterator.hasNext() && !abortStream.get()) {
                    getConsumer().accept(iterator.next());
                }
            }

            LOG.debug(loggingPrefix + "Exit polling loop with startRange: [{}] and endRange: [{}]. Sleeping for {}",
                    startRange,
                    endRange,
                    getPollingInterval().toString());

            startRange = endRange + 1; // endRange is inclusive in the raw request, so we must bump the startRange
            // Sleep for a polling interval
            try {
                Thread.sleep(getPollingInterval().toMillis());
            } catch (Exception e) {
                LOG.warn(loggingPrefix + "Exception when reading: " + e.toString());
                abortStream.set(true);
            }
        }
        state = State.STOPPED;
        return !abortStream.get();
    }

    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder setRawRows(RawRows value);
        abstract Builder setPollingInterval(Duration value);
        abstract Builder setPollingOffset(Duration value);
        abstract Builder setRawDbName(String value);
        abstract Builder setRawTableName(String value);
        abstract Builder setStartTime(Instant value);
        abstract Builder setEndTime(Instant value);
        abstract Builder setConsumer(Consumer<List<RawRow>> value);

        abstract RawPublisher build();
    }

    enum State {
        READY,
        RUNNING,
        STOPPED
    }
}