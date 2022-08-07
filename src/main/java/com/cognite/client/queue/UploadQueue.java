package com.cognite.client.queue;

import com.cognite.client.Request;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
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

    protected static final Duration MIN_MAX_UPLOAD_INTERVAL = Duration.ofSeconds(1L);
    protected static final Duration DEFAULT_MAX_UPLOAD_INTERVAL = Duration.ofSeconds(10L);
    protected static final Duration MAX_MAX_UPLOAD_INTERVAL = Duration.ofMinutes(60L);

    protected static final int DEFAULT_QUEUE_SIZE = 10_000;
    protected static final float QUEUE_FILL_RATE_THRESHOLD = 0.8f;

    // Internal state
    protected AtomicBoolean stopStream = new AtomicBoolean(false);

    private static <T> Builder<T> builder() {
        return new AutoValue_UploadQueue.Builder<T>()
                .setScheduledExecutor(new ScheduledThreadPoolExecutor(1))
                .setQueue(new ArrayBlockingQueue<T>(DEFAULT_QUEUE_SIZE))
                .setMaxUploadInterval(DEFAULT_MAX_UPLOAD_INTERVAL)
                ;
    }

    public static <T> UploadQueue<T> of(UpsertTarget<T> target) {
        return UploadQueue.<T>builder()
                .setUpsertTarget(target)
                .build();
    }

    abstract Builder<T> toBuilder();

    abstract ScheduledThreadPoolExecutor getScheduledExecutor();
    abstract Duration getMaxUploadInterval();
    abstract BlockingQueue<T> getQueue();
    @Nullable
    abstract Consumer<List<T>> getPostUploadFunction();

    @Nullable
    abstract Consumer<? extends Throwable> getExceptionHandlerFunction();


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
     * @return The {@link UploadQueue} with the function configured.
     */
    public UploadQueue<T> withPostUploadFunction(Consumer<List<T>> function) {
        return toBuilder().setPostUploadFunction(function).build();
    }

    /**
     * Add an exception handler function.
     *
     * The exception handler function will be called in case of an exception during uploading objects to
     * Cognite Data Fusion.
     *
     * We highly recommend that you add the exception handling function--if not, you risk an upload failing silently.
     *
     * @param function The function to call in case of an exception during upload.
     * @return The {@link UploadQueue} with the function configured.
     */
    public UploadQueue<T> withExceptionHandlerFunction(Consumer<? extends Throwable> function) {
        return toBuilder().setExceptionHandlerFunction(function).build();
    }

    /**
     * Sets the queue size.
     *
     * The queue size is the maximum number of elements that the queue can hold before starting to block on {@code put}
     * operations.
     *
     * The queue will automatically be uploaded when it is 80% full, so you should set the queue size to slightly larger
     * than your desired max batch size.
     *
     * The default queue size is 10k.
     * @param queueSize The target queue size.
     * @return The {@link UploadQueue} with the consumer configured.
     */
    public UploadQueue<T> withQueueSize(int queueSize) {
        Preconditions.checkArgument(queueSize > 0, "The queue size must be a positive integer.");
        return toBuilder().setQueue(new ArrayBlockingQueue<>(queueSize)).build();
    }

    /**
     * Sets the max upload interval.
     *
     * If you have activated the The queue will be uploaded to
     *
     * The default max upload interval is 10 seconds.
     * @param interval The target max upload interval.
     * @return The {@link UploadQueue} with the upload interval configured.
     */
    public UploadQueue<T> withMaxUploadInterval(Duration interval) {
        Preconditions.checkArgument(interval.compareTo(MAX_MAX_UPLOAD_INTERVAL) <= 0
                && interval.compareTo(MIN_MAX_UPLOAD_INTERVAL) >= 0,
                String.format("The max upload interval can be minimum %s and maxmimum %s",
                        MIN_MAX_UPLOAD_INTERVAL, MAX_MAX_UPLOAD_INTERVAL));
        return toBuilder().setMaxUploadInterval(interval).build();
    }

    public void put(T element) throws InterruptedException {
        getQueue().put(element);

        // Check the current no elements of the queue and trigger an upload if the fill rate is above threshold
        // The upload will happen on a separate thread.
        if ((getQueue().size() / (getQueue().size() + getQueue().remainingCapacity())) > QUEUE_FILL_RATE_THRESHOLD) {

        }

    }

    /*
    Private convenience method to decorate async upload with post upload and exception handling functions.
     */
    private void asyncUploadWrapper() {
        String logPrefix = "asyncUploadWrapper() - ";
        try {
            List<T> uploadResults = this.upload();
            if (null != getPostUploadFunction()) {
                getPostUploadFunction().accept(uploadResults);
            }
        } catch (Exception e) {
            LOG.warn(logPrefix + "Exception during upload of the queue: {}", e);
            if (null != getExceptionHandlerFunction()) {
                getExceptionHandlerFunction().accept(e);
            } else {
                throw new RuntimeException(e);
            }
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
        String logPrefix = "upload() - ";
        if (getQueue().isEmpty()) {
            LOG.info(logPrefix + "The queue is empty--will skip upload.");
            return List.of();
        }

        List<T> uploadBatch = new ArrayList<>(getQueue().size());
        List<T> uploadResults = new ArrayList<>(getQueue().size());

        // drain the queue
        getQueue().drainTo(uploadBatch);

        // upload to the configured target
        if (null != getUpsertTarget()) {
            uploadResults = getUpsertTarget().upsert(uploadBatch);
        } else {
            LOG.warn(logPrefix + "No valid upload target configured for this queue.");
        }

        LOG.info(logPrefix + "Uploaded {} elements to CDF.", uploadBatch.size());
        return uploadResults;
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
        abstract Builder<T> setScheduledExecutor(ScheduledThreadPoolExecutor value);
        abstract Builder<T> setQueue(BlockingQueue<T> value);
        abstract Builder<T> setMaxUploadInterval(Duration value);
        abstract Builder<T> setPostUploadFunction(Consumer<List<T>> value);
        abstract Builder<T> setExceptionHandlerFunction(Consumer<? extends Throwable> value);
        abstract Builder<T> setUpsertTarget(UpsertTarget<T> value);

        abstract ScheduledThreadPoolExecutor getScheduledExecutor();

        abstract UploadQueue<T> autoBuild();
        final UploadQueue<T> build() {
            // Make sure the thread pool puts its threads to sleep to allow the JVM to exit without manual
            // clean-up from the client.
            ScheduledThreadPoolExecutor executor = getScheduledExecutor();
            executor.setKeepAliveTime(2000, TimeUnit.SECONDS);
            executor.allowCoreThreadTimeOut(true);
            setScheduledExecutor(executor);

            return autoBuild();
        }
    }
}