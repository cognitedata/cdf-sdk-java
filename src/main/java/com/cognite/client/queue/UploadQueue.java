package com.cognite.client.queue;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * The UploadQueue batches together items and uploads them to Cognite Data Fusion (CDF), both to minimize
 * the load on the API, and also to improve throughput.
 *
 * The queue is uploaded to CDF on three conditions:
 * 1) When the queue is 80% full.
 * 2) At a set interval (default is every 10 seconds).
 * 3) When the {@link #upload()} method is called.
 *
 * The queue is always uploaded when 80% full. This happens on a background thread so your client can keep putting items
 * on the queue while the upload runs in the background.
 *
 * The upload interval trigger must be explicitly enabled by calling the {@link #start()} method. This starts a
 * background task that triggers a queue upload at a configurable interval. The default interval is every 10 seconds.
 * The trigger interval works in combination with the fill rate interval--this is the recommended way to use the
 * queue. When you are done using the queue, you should call {@link #stop()} for proper cleanup of background tasks
 * and draining the queue.
 *
 * You can also trigger an upload manually by calling {@link #upload()}. This is a blocking function.
 *
 *
 * @param <T> The CDF resource type to upload.
 */
@AutoValue
public abstract class UploadQueue<T, R> implements AutoCloseable {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected static final Duration MIN_MAX_UPLOAD_INTERVAL = Duration.ofSeconds(1L);
    protected static final Duration DEFAULT_MAX_UPLOAD_INTERVAL = Duration.ofSeconds(10L);
    protected static final Duration MAX_MAX_UPLOAD_INTERVAL = Duration.ofMinutes(60L);

    protected static final int DEFAULT_QUEUE_SIZE = 10_000;
    protected static final float QUEUE_FILL_RATE_THRESHOLD = 0.8f;

    // Internal state
    protected final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    protected ScheduledFuture<?> recurringTask;

    {
        // Make sure the thread pool puts its threads to sleep to allow the JVM to exit without manual
        // clean-up from the client.
        executor.setKeepAliveTime(2, TimeUnit.SECONDS);
        executor.allowCoreThreadTimeOut(true);
        executor.setRemoveOnCancelPolicy(true);
    }

    private static <T, R> Builder<T, R> builder() {
        return new AutoValue_UploadQueue.Builder<T, R>()
                .setQueue(new ArrayBlockingQueue<T>(DEFAULT_QUEUE_SIZE))
                .setMaxUploadInterval(DEFAULT_MAX_UPLOAD_INTERVAL)
                ;
    }

    /**
     * Builds an upload queue for batching and pushing items to the provided {@link UpsertTarget}.
     *
     * @param target The sink to push data items to.
     * @return The {@code UploadQueue}
     * @param <T> The input type of elements put on the queue.
     * @param <R> The output type of elements posted to Cognite Data Fusion. I.e. the objects sent to the
     * {@code postUploadFunction}.
     */
    public static <T, R> UploadQueue<T, R> of(UpsertTarget<T, R> target) {
        return UploadQueue.<T, R>builder()
                .setUpsertTarget(target)
                .build();
    }

    /**
     * Builds an upload queue for batching and pushing items to the provided {@link UploadTarget}.
     *
     * @param target The sink to push data items to.
     * @return The {@code UploadQueue}
     * @param <T> The input type of elements put on the queue.
     * @param <R> The output type of elements posted to Cognite Data Fusion. I.e. the objects sent to the
     * {@code postUploadFunction}.
     */
    public static <T, R> UploadQueue<T, R> of(UploadTarget<T, R> target) {
        return UploadQueue.<T, R>builder()
                .setUploadTarget(target)
                .build();
    }

    abstract Builder<T, R> toBuilder();

    abstract Duration getMaxUploadInterval();
    abstract BlockingQueue<T> getQueue();
    @Nullable
    abstract Consumer<List<R>> getPostUploadFunction();

    @Nullable
    abstract Consumer<Exception> getExceptionHandlerFunction();


    @Nullable
    abstract UpsertTarget<T, R> getUpsertTarget();
    @Nullable
    abstract UploadTarget<T, R> getUploadTarget();

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
    public UploadQueue<T, R> withPostUploadFunction(Consumer<List<R>> function) {
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
    public UploadQueue<T, R> withExceptionHandlerFunction(Consumer<Exception> function) {
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
    public UploadQueue<T, R> withQueueSize(int queueSize) {
        Preconditions.checkArgument(queueSize > 0, "The queue size must be a positive integer.");
        return toBuilder().setQueue(new ArrayBlockingQueue<>(queueSize)).build();
    }

    /**
     * Sets the max upload interval.
     *
     * When you activate the queue background thread via {@link #start()}, the queue will be uploaded to
     * Cognite Data Fusion at least every upload interval (in addition to the upload triggered at 80% queue fill rate).
     *
     * The default max upload interval is 10 seconds.
     * @param interval The target max upload interval.
     * @return The {@link UploadQueue} with the upload interval configured.
     */
    public UploadQueue<T, R> withMaxUploadInterval(Duration interval) {
        Preconditions.checkArgument(interval.compareTo(MAX_MAX_UPLOAD_INTERVAL) <= 0
                && interval.compareTo(MIN_MAX_UPLOAD_INTERVAL) >= 0,
                String.format("The max upload interval can be minimum %s and maxmimum %s",
                        MIN_MAX_UPLOAD_INTERVAL, MAX_MAX_UPLOAD_INTERVAL));
        return toBuilder().setMaxUploadInterval(interval).build();
    }

    /**
     * Adds an element to the upload queue, waiting if necessary for space to become available.
     *
     * Under normal operating conditions, this method will add the element to the queue and return immediately to
     * the caller. The upload queue will automatically upload its elements to Cognite Data Fusion when the queue is
     * 80% full. The upload happens on a background thread and does not block the caller. However, if you over time
     * add elements to the queue at a higher rate than it can drain itself, you may be blocked to wait for space to
     * become available--this is a backpressure mechanism to prevent CDF from becoming overloaded.
     *
     * @see java.util.concurrent.BlockingQueue
     * @param element The data element to add to the queue
     * @throws InterruptedException if interrupted while waiting
     */
    public void put(T element) throws InterruptedException {
        String logPrefix = "put() - ";
        getQueue().put(element);

        /*
        Check the current no elements of the queue and trigger an upload if the fill rate is above threshold, and
        there are no upload requests in the background task queue. We have to check the background task queue so that
        we prevent overprovisioning tasks.

        The upload will happen on a separate thread.
         */
        if (((float) getQueue().size() / ((float) getQueue().size() + (float) getQueue().remainingCapacity())) > QUEUE_FILL_RATE_THRESHOLD
                && executor.getQueue().size() < 2) {
            LOG.debug(logPrefix + "Will trigger queue upload. Fill rate is above threshold. Queue capacity: {}, "
                    + "queue size: {}, remaining capacity: {}",
                    getQueue().size() + getQueue().remainingCapacity(),
                    getQueue().size(),
                    getQueue().remainingCapacity());

            executor.execute(this::asyncUploadWrapper);
        }
    }

    /**
     * Start the upload thread to perform an upload every {@code maxUploadInterval}. The default upload interval
     * is every 10 seconds.
     *
     * If the upload thread has already been started (for example by an earlier call to {@code start()} then this
     * method does nothing and returns {@code false}.
     *
     * @return {@code true} if the upload thread started successfully, {@code false} if the upload thread has already
     *  been started.
     */
    public boolean start() {
        String logPrefix = "start() - ";
        if (null != recurringTask) {
            // The upload task has already been started.
            LOG.warn(logPrefix + "The upload thread has already been started. Start() has no effect.");
            return false;
        }

        recurringTask = executor.scheduleWithFixedDelay(this::asyncUploadWrapper,
                1, getMaxUploadInterval().getSeconds(), TimeUnit.SECONDS);
        LOG.info(logPrefix + "Starting background upload thread to upload at interval {}", getMaxUploadInterval());
        return true;
    }

    /**
     * A mirror of the {@link #stop()} method to support auto close in a {@code try-with-resources} statement.
     *
     * @see #stop()
     */
    @Override
    public void close() throws Exception {
        this.stop();
    }

    /**
     * Stops the upload thread if it is running, waits for all current uploads to finish
     * and ensures the upload queue is empty by calling {@code upload()} one
     * last time after shutting down the thread.
     *
     * @return {@code true} if the upload thread stopped successfully, {@code false} if the upload thread was not started
     * in the first place.
     * @throws InterruptedException if interrupted before while processing.
     */
    public boolean stop() throws InterruptedException {
        String logPrefix = "stop() -";
        if (null == recurringTask) {
            // The upload task has not been started.
            LOG.info(logPrefix + "The upload thread has not been started. Stop() has no effect.");
            LOG.info(logPrefix + "Waiting for currently running upload tasks to finish.");
            this.awaitUploads(1, TimeUnit.HOURS);
            return false;
        }

        recurringTask.cancel(false);
        boolean returnValue = recurringTask.isDone();
        if (recurringTask.isDone()) {
            // cancellation of task was successful
            recurringTask = null;
            LOG.info(logPrefix + "Successfully stopped the background upload thread.");
            LOG.info(logPrefix + "Waiting for currently running upload tasks to finish.");
            this.awaitUploads(1, TimeUnit.HOURS);
        } else {
            LOG.warn(logPrefix + "Something went wrong when trying to stop the upload thread. Status isDone: {}, "
                    + "isCanceled: {}",
                    recurringTask.isDone(),
                    recurringTask.isCancelled());
        }
        return returnValue;
    }

    /**
     * Blocks until all current data in the queue has been uploaded, or the timeout occurs,
     * or the current thread is interrupted, whichever happens first.
     *
     * @param timeout the maximum timeout to wait.
     * @param unit the unit of the timout argument.
     * @return {@code true} if all uploads have completed and {@code false} if the timeout elapsed before termination.
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean awaitUploads(long timeout, TimeUnit unit) throws InterruptedException {
        String loggingPrefix = "awaitUploads() - ";
        Instant methodCallInstant = Instant.now();
        boolean finishedUploads = false;

        // Submit an upload task if there are data items in the queue and no upload tasks are waiting to start.
        if (getQueue().size() > 0 && executor.getQueue().isEmpty()) {
            executor.submit(this::asyncUploadWrapper);
        }

        // Check the status of all upload tasks and wait for them to finish--or abort when the timeout strikes.
        do {
            Thread.sleep(500);
            if (executor.getQueue().isEmpty() && executor.getActiveCount() == 0) {
                finishedUploads = true;
            } else {
                LOG.debug(loggingPrefix + "Uploads not finished. Upload tasks running: {}. Upload tasks queued: {}.",
                        executor.getActiveCount(),
                        executor.getQueue().size());
            }
        } while (!finishedUploads
                && Duration.between(methodCallInstant, Instant.now())
                        .compareTo(Duration.of(timeout, unit.toChronoUnit())) < 0);

        return finishedUploads;
    }

    /*
    Private convenience method to decorate async upload with post upload and exception handling functions.
     */
    private void asyncUploadWrapper() {
        String logPrefix = "asyncUploadWrapper() - ";
        try {
            List<R> uploadResults = this.upload();
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
     * This method will block the calling thread until the upload operation completes. It does not call the
     * {@code postUploadFunction} or {@code exceptionHandlerFunction}. The uploaded elements are returned directly
     * from this method.
     *
     * @return The uploaded objects.
     * @throws Exception in case of an error during the upload.
     */
    public List<R> upload() throws Exception {
        String logPrefix = "upload() - ";
        if (getQueue().isEmpty()) {
            LOG.info(logPrefix + "The queue is empty--will skip upload.");
            return List.of();
        }

        List<T> uploadBatch = new ArrayList<>(getQueue().size());
        List<R> uploadResults = new ArrayList<>(getQueue().size());

        // drain the queue
        getQueue().drainTo(uploadBatch);

        // upload to the configured target
        if (null != getUpsertTarget()) {
            uploadResults = getUpsertTarget().upsert(uploadBatch);
        } else if (null != getUploadTarget()) {
            uploadResults = getUploadTarget().upload(uploadBatch);
        } else {
            LOG.warn(logPrefix + "No valid upload target configured for this queue.");
        }

        LOG.info(logPrefix + "Uploaded {} elements to CDF.", uploadBatch.size());
        return uploadResults;
    }

    @AutoValue.Builder
    abstract static class Builder<T, R> {
        abstract Builder<T, R> setQueue(BlockingQueue<T> value);
        abstract Builder<T, R> setMaxUploadInterval(Duration value);
        abstract Builder<T, R> setPostUploadFunction(Consumer<List<R>> value);
        abstract Builder<T, R> setExceptionHandlerFunction(Consumer<Exception> value);
        abstract Builder<T, R> setUpsertTarget(UpsertTarget<T, R> value);
        abstract Builder<T, R> setUploadTarget(UploadTarget<T, R> value);

        abstract UploadQueue<T, R> build();
    }
}