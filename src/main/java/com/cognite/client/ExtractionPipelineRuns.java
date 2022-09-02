/*
 * Copyright (c) 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.ExtractionPipelineRun;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.ExtractionPipelineParser;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite extraction pipelines api endpoint.
 *
 * It provides methods for reading and writing {@link ExtractionPipelineRuns}.
 */
@AutoValue
public abstract class ExtractionPipelineRuns extends ApiBase {

    private static Builder builder() {
        return new AutoValue_ExtractionPipelineRuns.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(ExtractionPipelineRuns.class);

    /**
     * Constructs a new {@link ExtractionPipelineRuns} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static ExtractionPipelineRuns of(CogniteClient client) {
        return ExtractionPipelineRuns.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link ExtractionPipelineRun} objects for a given
     * {@link com.cognite.client.dto.ExtractionPipeline} external id.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<ExtractionPipelineRun> listResults = new ArrayList<>();
     *     client.extractionPipelines()
     *             .runs()
     *             .list("my-extraction-pipeline-external-id)
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Extraction-Pipelines-Runs/operation/filterRuns">API Reference - Filter extraction pipeline runs</a>
     *
     * @see #list(Request)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     * @see ExtractionPipelines#runs()
     * @param extractionPipelineExtId the external id of the extraction pipeline to list runs for.
     * @return an {@link Iterator} to page through the results set.
     */
    public Iterator<List<ExtractionPipelineRun>> list(String extractionPipelineExtId) throws Exception {
        return this.list(Request.create().withFilterParameter("externalId", extractionPipelineExtId));
    }

    /**
     * Returns all {@link ExtractionPipelineRun} objects that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<ExtractionPipelineRun> listResults = new ArrayList<>();
     *      client.extractionPipelines()
     *              .runs()
     *              .list(Request.create()
     *                             .withFilterParameter("statuses", List.of("success", "failure", "seen")))
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Extraction-Pipelines-Runs/operation/filterRuns">API Reference - Filter extraction pipeline runs</a>
     *
     * @see #list(Request,String...)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     * @see ExtractionPipelines#runs()
     *
     * @param requestParameters the filters to use for retrieving the extraction pipeline runs.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ExtractionPipelineRun>> list(Request requestParameters) throws Exception {
        /*
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
         */

        // The extraction pipeline API endpoint does not support partitions (yet). Therefore no partitions are used here
        // in the list implementation.

        return list(requestParameters, new String[0]);
    }

    /**
     * Returns all {@link ExtractionPipelineRun} objects that matches the filters set in the {@link Request} for the
     * specified partitions. This is method is intended for advanced use cases where you need direct control over
     * the individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<ExtractionPipelineRun> listResults = new ArrayList<>();
     *      client.extractionPipelines()
     *              .runs()
     *              .list(Request.create()
     *                             .withFilterParameter("statuses", List.of("success", "failure", "seen")),
     *                                  "1/8","2/8","3/8","4/8","5/8","6/8","7/8","8/8")
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Extraction-Pipelines-Runs/operation/filterRuns">API Reference - Filter extraction pipeline runs</a>
     *
     * @see #listJson(ResourceType,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     * @see ExtractionPipelines#runs()
     *
     * @param requestParameters the filters to use for retrieving the extraction pipeline runs.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ExtractionPipelineRun>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.EXTRACTION_PIPELINE_RUN, requestParameters, partitions),
                this::parseExtractionPipelineRun);
    }

    /**
     * Creates a set of {@link ExtractionPipelineRun} objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<ExtractionPipelineRun> runs = // List of Extraction Pipeline Run;
     *      client.extractionPipelines().runs().upsert(runs);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Extraction-Pipelines-Runs/operation/createRuns">API Reference - Create extraction pipeline runs</a>
     *
     * @see UpsertItems#create(List)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     * @see ExtractionPipelines#runs()
     *
     * @param runs The extraction pipelines to create.
     * @return The created extraction pipelines.
     * @throws Exception
     */
    public List<ExtractionPipelineRun> create(List<ExtractionPipelineRun> runs) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeExtractionPipelineRuns();
        //ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateExtractionPipelines();

        UpsertItems<ExtractionPipelineRun> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withIdFunction(this::getExtractionPipelineId)
                .withMaxBatchSize(1);

        return upsertItems.create(runs).stream()
                .map(this::parseExtractionPipelineRun)
                .collect(Collectors.toList());
    }

    /**
     * Create an extraction pipeline heartbeat object.
     *
     * The heartbeat object will post a regular heartbeat to a specified extraction pipeline. You start the
     * heartbeat by calling {@link Heartbeat#start()}. This will start a background thread posting a heartbeat to
     * Cognite Data Fusion until you call {@link Heartbeat#stop()}.
     *
     * You can also push a single heartbeat by calling {@link Heartbeat#sendHeartbeat()}.
     *
     * @param extractionPipelineExtId The external id of the {@code extraction pipeline} to post the heartbeat to.
     * @return the heartbeat object.
     */
    public Heartbeat heartbeat(String extractionPipelineExtId) {
        return Heartbeat.of(getClient(), extractionPipelineExtId);
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private ExtractionPipelineRun parseExtractionPipelineRun(String json) {
        try {
            return ExtractionPipelineParser.parseExtractionPipelineRun(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(ExtractionPipelineRun item) {
        try {
            return ExtractionPipelineParser.toRequestInsertItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of an extraction pipeline. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getExtractionPipelineId(ExtractionPipelineRun item) {
        if (item.hasExternalId()) {
            return Optional.of(item.getExternalId());
        } else if (item.hasId()) {
            return Optional.of(String.valueOf(item.getId()));
        } else {
            return Optional.<String>empty();
        }
    }

    /**
     * An object that can issue a regular "heartbeat" by posting regular {@link ExtractionPipelineRun} with
     * {@code seen} status to Cognite Data Fusion.
     */
    @AutoValue
    public abstract static class Heartbeat implements Closeable {
        protected static final Duration MIN_INTERVAL = Duration.ofSeconds(10L);
        protected static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(60L);
        protected static final Duration MAX_INTERVAL = Duration.ofMinutes(60L);

        protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

        protected final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        protected ScheduledFuture<?> recurringTask;

        {
            // Make sure the thread pool puts its threads to sleep to allow the JVM to exit without manual
            // clean-up from the client.
            executor.setKeepAliveTime(2000, TimeUnit.SECONDS);
            executor.allowCoreThreadTimeOut(true);
            executor.setRemoveOnCancelPolicy(true);
        }

        private static Builder builder() {
            return new AutoValue_ExtractionPipelineRuns_Heartbeat.Builder()
                    .setInterval(DEFAULT_INTERVAL);
        }

        /**
         * Create the heartbeat object.
         *
         * The heartbeat object will post a regular heartbeat to a specified extraction pipeline. You start the
         * heartbeat by calling {@link #start()}. This will start a background thread posting a heartbeat to
         * Cognite Data Fusion until you call {@link #stop()}.
         *
         * You can also push a single heartbeat by calling {@link #sendHeartbeat()}.
         *
         * @param client The cognite client for connecting to CDF.
         * @param extractionPipelineExtId The external id of the {@code extraction pipeline} to post the heartbeat to.
         * @return the heartbeat object.
         */
        public static Heartbeat of(CogniteClient client,
                                   String extractionPipelineExtId) {
            return builder()
                    .setCogniteClient(client)
                    .setExtractionPipelineExtId(extractionPipelineExtId)
                    .build();
        }

        abstract Builder toBuilder();
        abstract Duration getInterval();
        abstract CogniteClient getCogniteClient();
        abstract String getExtractionPipelineExtId();

        /**
         * Sets the heartbeat interval.
         *
         * When you activate the heartbeat thread via {@link #start()}, a {@link ExtractionPipelineRun} with state
         * {@code seen} will be uploaded to Cognite Data Fusion at every heartbeat interval.
         *
         * The default heartbeat interval is 60 seconds.
         * @param interval The target heartbeat interval.
         * @return The {@link Heartbeat} with the upload interval configured.
         */
        public Heartbeat withInterval(Duration interval) {
            Preconditions.checkArgument(interval.compareTo(MAX_INTERVAL) <= 0
                            && interval.compareTo(MIN_INTERVAL) >= 0,
                    String.format("The max upload interval can be minimum %s and maxmimum %s",
                            MIN_INTERVAL, MAX_INTERVAL));
            return toBuilder().setInterval(interval).build();
        }

        /**
         * Push a heartbeat {@code ExtractionPipelineRun.Status.SEEN} to Cognite Data Fusion.
         *
         * @throws Exception
         */
        public void sendHeartbeat() throws Exception {
            ExtractionPipelineRun pipelineRun = ExtractionPipelineRun.newBuilder()
                    .setExternalId(getExtractionPipelineExtId())
                    .setCreatedTime(Instant.now().toEpochMilli())
                    .setStatus(ExtractionPipelineRun.Status.SEEN)
                    .build();

            getCogniteClient().extractionPipelines().runs().create(List.of(pipelineRun));
        }

        /*
            Private convenience method to decorate async heartbeat with exception handling.
        */
        private void asyncHeartbeatWrapper() {
            String logPrefix = "asyncHeartbeatWrapper() - ";
            try {
                sendHeartbeat();
            } catch (Exception e) {
                LOG.error(logPrefix + "Exception during heartbeat {}", e);
                throw new RuntimeException(e);
            }
        }

        /**
         * Start the heartbeat thread to perform an upload every {@code interval}. The default heartbeat interval
         * is every 60 seconds.
         *
         * If the heartbeat thread has already been started (for example by an earlier call to {@code start()} then this
         * method does nothing and returns {@code false}.
         *
         * @return {@code true} if the heartbeat thread started successfully, {@code false} if the heartbeat thread has already
         *  been started.
         */
        public boolean start() {
            String logPrefix = "start() - ";
            if (null != recurringTask) {
                // The upload task has already been started.
                LOG.warn(logPrefix + "The commit thread has already been started. Start() has no effect.");
                return false;
            }

            recurringTask = executor.scheduleAtFixedRate(this::asyncHeartbeatWrapper,
                    1, getInterval().getSeconds(), TimeUnit.SECONDS);
            LOG.info(logPrefix + "Starting background thread to commit state at interval {}", getInterval());
            return true;
        }

        /**
         * A mirror of the {@link #stop()} method to support auto close in a {@code try-with-resources} statement.
         *
         * @see #stop()
         */
        @Override
        public void close() {
            this.stop();
        }

        /**
         * Stops the heartbeat thread if it is running.
         *
         * @return {@code true} if the heartbeat thread stopped successfully, {@code false} if the heartbeat thread
         * was not started in the first place.
         */
        public boolean stop() {
            String logPrefix = "stop() -";
            if (null == recurringTask) {
                // The upload task has not been started.
                LOG.warn(logPrefix + "The heartbeat thread has not been started. Stop() has no effect.");
                return false;
            }

            recurringTask.cancel(false);
            boolean returnValue = recurringTask.isDone();
            if (recurringTask.isDone()) {
                // cancellation of task was successful
                recurringTask = null;
                LOG.info(logPrefix + "Successfully stopped the background heartbeat thread.");
            } else {
                LOG.warn(logPrefix + "Something went wrong when trying to stop the heartbeat thread. Status isDone: {}, "
                                + "isCanceled: {}",
                        recurringTask.isDone(),
                        recurringTask.isCancelled());
            }
            return returnValue;
        }

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setInterval(Duration value);
            abstract Builder setExtractionPipelineExtId(String value);
            abstract Builder setCogniteClient(CogniteClient value);

            abstract Heartbeat build();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract ExtractionPipelineRuns build();
    }
}
