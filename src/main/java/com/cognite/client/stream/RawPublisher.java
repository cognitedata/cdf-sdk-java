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
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * This class produces a continuous data stream of rows from a raw table. The raw table is monitored for changes and
 * all new or changed rows are streamed.
 *
 * The publisher polls Raw for updates at the {@code pollingInterval} (default every 5 sec.) and push the resulting
 * batch of {@link RawRow} to the registered {@code Consumer}.
 *
 */
@AutoValue
public abstract class RawPublisher {
    protected static final Logger LOG = LoggerFactory.getLogger(RawPublisher.class);

    // Defaults and boundary values
    private static final Duration MIN_POLLING_INTERVAL = Duration.ofMillis(500L);
    private static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofSeconds(5L);
    private static final Duration MAX_POLLING_INTERVAL = Duration.ofSeconds(60L);

    private static final Duration MIN_POLLING_OFFSET = Duration.ofMillis(500L);
    private static final Duration DEFAULT_POLLING_OFFSET = Duration.ofSeconds(2L);
    private static final Duration MAX_POLLING_OFFSET = Duration.ofDays(10L);

    private static final Instant MIN_START_TIME = Instant.EPOCH;
    // Have to subtract to guard against overflow
    private static final Instant MAX_END_TIME = Instant.MAX.minus(MAX_POLLING_OFFSET).minusSeconds(1);

    // Internal state
    private AtomicBoolean abortStream = new AtomicBoolean(false);
    private State state = State.READY;

    private static Builder builder() {
        return new AutoValue_RawPublisher.Builder()
                .setPollingInterval(DEFAULT_POLLING_INTERVAL)
                .setPollingOffset(DEFAULT_POLLING_OFFSET)
                .setStartTime(MIN_START_TIME.plusSeconds(1))
                .setEndTime(MAX_END_TIME)
                ;
    }

    /**
     * For internal use.
     *
     * Configures a publisher to stream rows from the specified raw table.
     *
     * @param rawRows The read raw rows api to use for querying Raw.
     * @param rawDbName The raw database to read from.
     * @param rawTableName The raw table to read from.
     * @return The configured {@link RawPublisher}
     */
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
     * @return The {@link RawPublisher} with the consumer configured.
     */
    public RawPublisher withConsumer(Consumer<List<RawRow>> consumer) {
        return toBuilder().setConsumer(consumer).build();
    }

    /**
     * Sets the start time (i.e. the earliest possible created/changed time of the CDF Raw Row) of the data stream.
     *
     * The default start time is at Unix epoch. I.e. the publisher will read all existing rows (if any) in the raw table.
     * @param startTime The start time instant
     * @return The {@link RawPublisher} with the consumer configured.
     */
    public RawPublisher withStartTime(Instant startTime) {
        Preconditions.checkArgument(startTime.isAfter(MIN_START_TIME) && startTime.isBefore(MAX_END_TIME),
                "Start time must be after Unix Epoch and before Instant.MAX.minus(1, ChronoUnit.YEARS).");
        return toBuilder().setStartTime(startTime).build();
    }

    /**
     * Sets the end time (i.e. the latest possible created/changed time of the CDF Raw Row) of the data stream.
     *
     * The default end time is {@code Instant.MAX}. I.e. the publisher will stream data indefinitely, or until
     * aborted.
     * @param endTime The end time instant
     * @return The {@link RawPublisher} with the consumer configured.
     */
    public RawPublisher withEndTime(Instant endTime) {
        Preconditions.checkArgument(endTime.isAfter(MIN_START_TIME) && endTime.isBefore(MAX_END_TIME),
                "End time must be after Unix Epoch and before Instant.MAX.minus(1, ChronoUnit.YEARS).");
        return toBuilder().setEndTime(endTime).build();
    }

    /**
     * Sets the polling interval to check for updates to the source raw table. The default polling interval is
     * every 5 seconds. You can configure a more or less frequent interval, down to every 0.5 seconds.
     *
     * @param interval The interval to check the source raw table for updates.
     * @return The {@link RawPublisher} with the consumer configured.
     */
    public RawPublisher withPollingInterval(Duration interval) {
        Preconditions.checkArgument(interval.compareTo(MIN_POLLING_INTERVAL) > 0
                        && interval.compareTo(MAX_POLLING_INTERVAL) < 0,
                String.format("Polling interval must be greater than %s and less than %s.",
                        MIN_POLLING_INTERVAL,
                        MAX_POLLING_INTERVAL));
        return toBuilder().setPollingInterval(interval).build();
    }

    /**
     * Sets the polling offset. The offset is a time window "buffer" subtracted from the current time when polling
     * for data from CDF Raw. It is intended as a safeguard for clock differences between the client (running this
     * publisher) and the CDF service.
     *
     * For example, if the polling offset is 2 seconds, then this publisher will look for data updates up to (and including)
     * T-2 seconds. That is, data will be streamed with a 2 second fixed latency/delay.
     *
     * @param interval The interval to check the source raw table for updates.
     * @return The {@link RawPublisher} with the consumer configured.
     */
    public RawPublisher withPollingOffset(Duration interval) {
        Preconditions.checkArgument(interval.compareTo(MIN_POLLING_OFFSET) > 0
                        && interval.compareTo(MAX_POLLING_OFFSET) < 0,
                String.format("Polling offset must be greater than %s and less than %s.",
                        MIN_POLLING_OFFSET,
                        MAX_POLLING_OFFSET));
        return toBuilder().setPollingOffset(interval).build();
    }

    /**
     * Starts the streaming job.
     *
     * The job is executed on a separate thread and this method will immediately return to the caller. It returns
     * a {@link Future} that you can use to block the execution of your own code if you want to explicitly
     * wait for completion of the streaming job.
     *
     * @return A Future hosting the end state of the streaming job. The future returns {@code true} when the
     * polling loop completes (at its specified end time). {@code false} if the job is aborted before the
     * specified end time.
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
        LOG.info("Publisher aborted.");
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
        LOG.info(loggingPrefix + "Setting up streaming read from CDF.Raw: [{}.{}]. Time window start: {}. End: {}",
                getRawDbName(),
                getRawTableName(),
                getStartTime().toString(),
                getEndTime().toString());
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
                    List<RawRow> batch = iterator.next();
                    if (batch.size() > 0) {
                        getConsumer().accept(batch);
                    }
                }
            }

            LOG.debug(loggingPrefix + "Finished polling loop with startRange: [{}] and endRange: [{}]. Sleeping for {}",
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