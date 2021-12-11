package com.cognite.client.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract superclass for streaming publishers. It holds key constants and variables.
 *
 */
public abstract class AbstractPublisher {
    // Defaults and boundary values
    protected static final Duration MIN_POLLING_INTERVAL = Duration.ofMillis(500L);
    protected static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofSeconds(5L);
    protected static final Duration MAX_POLLING_INTERVAL = Duration.ofSeconds(60L);

    protected static final Duration MIN_POLLING_OFFSET = Duration.ofMillis(500L);
    protected static final Duration DEFAULT_POLLING_OFFSET = Duration.ofSeconds(30L);
    protected static final Duration MAX_POLLING_OFFSET = Duration.ofDays(10L);

    protected static final Instant MIN_START_TIME = Instant.EPOCH;
    // Have to subtract to guard against overflow
    protected static final Instant MAX_END_TIME = Instant.MAX.minus(MAX_POLLING_OFFSET).minusSeconds(1);

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    // Internal state
    protected AtomicBoolean abortStream = new AtomicBoolean(false);
    protected AbstractPublisher.State state = AbstractPublisher.State.READY;

    protected abstract Duration getPollingInterval();
    protected abstract Duration getPollingOffset();
    protected abstract Instant getStartTime();
    protected abstract Instant getEndTime();

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
    abstract boolean run() throws Exception;

    abstract static class Builder<B extends Builder<B>> {
        abstract B setPollingInterval(Duration value);
        abstract B setPollingOffset(Duration value);
        abstract B setStartTime(Instant value);
        abstract B setEndTime(Instant value);
    }

    enum State {
        READY,
        RUNNING,
        STOPPED
    }
}