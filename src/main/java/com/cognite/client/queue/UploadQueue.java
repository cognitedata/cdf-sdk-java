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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
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

    protected static final int DEFAULT_QUEUE_SIZE = 20_000;

    protected static final int DEFAULT_BATCH_SIZE = 8_000;

    // Internal state
    protected AtomicBoolean stopStream = new AtomicBoolean(false);

    private static <T> Builder<T> builder() {
        return new AutoValue_UploadQueue.Builder<T>()
                .setQueue(new ArrayBlockingQueue<T>(DEFAULT_QUEUE_SIZE))
                .setBatchSize(DEFAULT_BATCH_SIZE)
                .setMaxUploadInterval(DEFAULT_MAX_UPLOAD_INTERVAL)
                ;
    }

    public static <T> UploadQueue<T> of(UpsertTarget<T> target) {
        return UploadQueue.<T>builder()
                .setUpsertTarget(target)
                .build();
    }

    abstract Builder<T> toBuilder();

    abstract int getBatchSize();
    abstract Duration getMaxUploadInterval();
    abstract BlockingQueue<T> getQueue();
    @Nullable
    abstract Consumer<List<T>> getPostUploadFunction();


    @Nullable
    abstract UpsertTarget<T> getUpsertTarget();

    /**
     * Add a post upload function.
     *
     * The post upload function will be called after the successful upload of a batch of data objects to
     * Cognite Data Fusion. The function will be given the list of objects that were uploaded.
     *
     * The post upload function has the potential to block the upload thread, so you should ensure that it is lightweight.
     * If you need to perform a costly operation, we recommend that you hand the costly operation over to a separate
     * thread and let the post upload function return quickly.
     *
     * @param function The function to call for each batch of {@code T}.
     * @return The {@link UploadQueue} with the consumer configured.
     */
    public UploadQueue<T> withPostUploadFunction(Consumer<List<T>> function) {
        return toBuilder().setPostUploadFunction(function).build();
    }

    /**
     * Sets the queue size.
     *
     * The queue size is the maximum number of elements that the queue can hold before starting to block on {@code put}
     * operations.
     *
     * The default queue size is 20k.
     * @param queueSize The target batch size.
     * @return The {@link UploadQueue} with the consumer configured.
     */
    public UploadQueue<T> withQueueSize(int queueSize) {
        Preconditions.checkArgument(queueSize > 0, "The queue size must be a positive integer.");
        return toBuilder().setQueue(new ArrayBlockingQueue<>(queueSize)).build();
    }

    /**
     * Sets the batch size for upload batches.
     *
     * When the number of elements in the queue reaches the batch size, it will trigger an automatic upload
     * (given that you have started the queue).
     *
     * The batch size must be less or equal to the queue size. We recommend using a queue size about double the
     * batch size for max performance in most scenarios.
     *
     * The default batch size is 8k.
     * @param batchSize The target batch size.
     * @return The {@link UploadQueue} with the consumer configured.
     */
    public UploadQueue<T> withMaxBatchSize(int batchSize) {
        Preconditions.checkArgument(batchSize <= (getQueue().remainingCapacity() + getQueue().size()),
                "Batch size must be less or equal to the queue size");
        return toBuilder().setBatchSize(batchSize).build();
    }

    public void put(T element) throws InterruptedException {
        getQueue().put(element);

        // Check the current no elements of the queue and trigger an upload on batch size
        // The upload will happen on a separate thread.
        if (getQueue().size() >= getBatchSize()) {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.submit(this::upload);
            executorService.shutdown();
        }

    }

    /**
     * Uploads the current elements in the queue.
     *
     * This method will block the calling thread until the upload operation completes.
     *
     * @return The uploaded objects.
     * @throws Exception in case of an error during the upload.
     */
    public List<T> upload() throws Exception {
        List<T> uploadBatch = new ArrayList<>(getQueue().size());
        List<T> uploadResults = new ArrayList<>(getQueue().size());

        // drain the queue
        getQueue().drainTo(uploadBatch);

        // upload to the configured target
        if (null != getUpsertTarget()) {
            uploadResults = getUpsertTarget().upsert(uploadBatch);
        }

        return uploadResults;
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
    abstract static class Builder<T> {
        abstract Builder<T> setQueue(BlockingQueue<T> value);
        abstract Builder<T> setBatchSize(int value);
        abstract Builder<T> setMaxUploadInterval(Duration value);
        abstract Builder<T> setPostUploadFunction(Consumer<List<T>> value);
        abstract Builder<T> setUpsertTarget(UpsertTarget<T> value);

        abstract UploadQueue<T> build();
    }
}