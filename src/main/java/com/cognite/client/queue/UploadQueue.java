package com.cognite.client.queue;

import com.cognite.client.Request;
import com.cognite.client.stream.AbstractPublisher;
import com.cognite.client.stream.AutoValue_Publisher;
import com.cognite.client.stream.ListSource;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 *
 * @param <T>
 */
@AutoValue
public abstract class UploadQueue<T> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected static final Duration POLLING_INTERVAL = Duration.ofSeconds(1L);

    protected static final Duration MIN_DEFAULT_MAX_UPLOAD_INTERVAL = Duration.ofSeconds(1L);
    protected static final Duration DEFAULT_MAX_UPLOAD_INTERVAL = Duration.ofSeconds(10L);
    protected static final Duration MAX_MAX_UPLOAD_INTERVAL = Duration.ofSeconds(60L);

    // Internal state
    protected AtomicBoolean stopStream = new AtomicBoolean(false);

    private static <T> Builder<T> builder() {
        return new AutoValue_UploadQueue.Builder<T>()
                .setPollingInterval(DEFAULT_POLLING_INTERVAL)
                .setPollingOffset(DEFAULT_POLLING_OFFSET)
                .setRequest(Request.create())
                ;
    }

    public static <T> UploadQueue<T> of(ListSource<T> listSource) {
        return UploadQueue.<T>builder()
                .setSource(listSource)
                .build();
    }

    abstract Builder<T> toBuilder();

    abstract ListSource<T> getSource();
    abstract Request getRequest();
    @Nullable
    abstract Consumer<List<T>> getConsumer();

    /**
     * Add the consumer of the data stream.
     *
     * The consumer will be called for each batch of {@code T}. This is potentially a blocking operation,
     * so you should take care to process the batch efficiently (or spin off processing to a separate thread).
     *
     * @param consumer The function to call for each batch of {@code T}.
     * @return The {@link UploadQueue} with the consumer configured.
     */
    public UploadQueue<T> withConsumer(Consumer<List<T>> consumer) {
        return toBuilder().setConsumer(consumer).build();
    }

    /**
     * Sets the start time (i.e. the earliest possible created/changed time of the CDF Raw Row) of the data stream.
     *
     * The default start time is at Unix epoch. I.e. the publisher will read all existing rows (if any) in the raw table.
     * @param startTime The start time instant
     * @return The {@link UploadQueue} with the consumer configured.
     */
    public UploadQueue<T> withStartTime(Instant startTime) {
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
     * @return The {@link UploadQueue} with the consumer configured.
     */
    public UploadQueue<T> withEndTime(Instant endTime) {
        Preconditions.checkArgument(endTime.isAfter(MIN_START_TIME) && endTime.isBefore(MAX_END_TIME),
                "End time must be after Unix Epoch and before Instant.MAX.minus(1, ChronoUnit.YEARS).");
        return toBuilder().setEndTime(endTime).build();
    }

    /**
     * Sets the polling interval to check for updates to the source raw table. The default polling interval is
     * every 5 seconds. You can configure a more or less frequent interval, down to every 0.5 seconds.
     *
     * @param interval The interval to check the source raw table for updates.
     * @return The {@link UploadQueue} with the consumer configured.
     */
    public UploadQueue<T> withPollingInterval(Duration interval) {
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
     * @return The {@link UploadQueue} with the consumer configured.
     */
    public UploadQueue<T> withPollingOffset(Duration interval) {
        Preconditions.checkArgument(interval.compareTo(MIN_POLLING_OFFSET) > 0
                        && interval.compareTo(MAX_POLLING_OFFSET) < 0,
                String.format("Polling offset must be greater than %s and less than %s.",
                        MIN_POLLING_OFFSET,
                        MAX_POLLING_OFFSET));
        return toBuilder().setPollingOffset(interval).build();
    }

    /**
     * Sets a baseline {@link Request} to use when producing the steam of objects. The {@link Request} contains
     * the set of filters that you want the (stream) objects to satisfy.
     *
     * @param request The baseline request specifying filters for the object stream.
     * @return The {@link UploadQueue} with the consumer configured.
     */
    public UploadQueue<T> withRequest(Request request) {
        Preconditions.checkArgument(0 == request.getRequestParameters().keySet()
                .stream()
                .filter(key -> !key.equals("filter"))
                .count(),
                "The request can only contain a filter node.");
        return toBuilder().setRequest(request).build();
    }


    /**
     * Start the main polling loop for reading rows from a raw table.
     *
     * @return {@code true} when the polling loop completes (at the specified end time). {@code false} if the
     * job is aborted before the specified end time.
     * @throws Exception
     */
    boolean run() throws Exception {
        final String loggingPrefix = "streaming() [" + RandomStringUtils.randomAlphanumeric(4) + "] - ";
        Preconditions.checkNotNull(getConsumer(),
                loggingPrefix + "You must specify a Consumer via withConsumer(Consumer<List<RawRow>>)");
        Preconditions.checkState(getStartTime().isBefore(getEndTime()),
                String.format(loggingPrefix + "Start time must be before end time. Start time: %s. End time: %s",
                        getStartTime(),
                        getEndTime()));
        LOG.info(loggingPrefix + "Setting up streaming read from {}. Time window start: {}. End: {}",
                getSource().getClass().getSimpleName(),
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
                Request query = getRequest()
                        .withFilterParameter("lastUpdatedTime", Map.of(
                                "min", startRange,
                                "max", endRange))
                        ;
                LOG.debug(loggingPrefix + "Send request to read CDF: {}",
                        query);

                Iterator<List<T>> iterator = getSource().list(query);
                while (iterator.hasNext() && !abortStream.get()) {
                    List<T> batch = iterator.next();
                    if (batch.size() > 0) {
                        getConsumer().accept(batch);
                    }
                }
            }

            LOG.debug(loggingPrefix + "Finished polling loop with startRange: [{}] and endRange: [{}]. Sleeping for {}",
                    startRange,
                    endRange,
                    getPollingInterval().toString());

            startRange = endRange + 1; // endRange is inclusive in the request, so we must bump the next startRange

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
    abstract static class Builder<T> extends AbstractPublisher.Builder<Builder<T>> {
        abstract Builder<T> setSource(ListSource<T> value);
        abstract Builder<T> setConsumer(Consumer<List<T>> value);
        abstract Builder<T> setRequest(Request value);

        abstract UploadQueue<T> build();
    }
}