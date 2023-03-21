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

import com.cognite.client.dto.EntityMatchModel;
import com.cognite.client.dto.EntityMatchResult;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.Connector;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.EntityMatchingParser;
import com.cognite.client.util.Partition;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.protobuf.Struct;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


/**
 * This class represents the Cognite entity matching api endpoint
 *
 * It provides methods for interacting with the entity matching services.
 */
@AutoValue
public abstract class EntityMatching extends ApiBase {

    private static Builder builder() {
        return new AutoValue_EntityMatching.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(EntityMatching.class);

    /**
     * Construct a new {@link EntityMatching} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return The datasets api object.
     */
    public static EntityMatching of(CogniteClient client) {
        return EntityMatching.builder()
                .setClient(client)
                .build();
    }

    /**
     * Matches a set of source entities with a set of targets via a given matching model.
     *
     * If either sources or targets are empty lists, the entity matcher will use
     * the sources/targets from the model training.
     *
     * The default number of matches is 1 and score threshold used for matching is 0.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     String modelExternalId = // modelExternalId;
     *     List<Struct> sources = // sources ;
     *     List<Struct> targets = // targets;
     *     List<EntityMatchResult> result = client.contextualization()
     *                     .entityMatching()
     *                     .predict(modelExternalId, sources, targets);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Entity-matching/operation/entityMatchingPredict">API Reference - Predict matches</a>
     *
     * @see CogniteClient
     * @see CogniteClient#contextualization()
     * @see Contextualization#entityMatching()
     * @see #predict(String, List, Collection, int)
     *
     * @param modelExternalId The external id of the matching model to use.
     * @param sources A list of entities to match from. If the list is empty, the model training sources will be used.
     * @param targets A list of entities to match to. If the list is empty, the model training targets will be used.
     * @return The entity matching results.
     * @throws Exception
     */
    public List<EntityMatchResult> predict(String modelExternalId,
                                           List<Struct> sources,
                                           Collection<Struct> targets) throws Exception {
        return predict(modelExternalId, sources, targets, 1);
    }

    /**
     * Matches a set of source entities with a set of targets via a given matching model.
     *
     * If either sources or targets are empty lists, the entity matcher will use
     * the sources/targets from the model training.
     *
     * The default score threshold used for matching is 0.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     String modelExternalId = // modelExternalId;
     *     List<Struct> sources = // sources ;
     *     List<Struct> targets = // targets;
     *     List<EntityMatchResult> result = client.contextualization()
     *                     .entityMatching()
     *                     .predict(modelExternalId, sources, targets, 1);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Entity-matching/operation/entityMatchingPredict">API Reference - Predict matches</a>
     *
     * @see CogniteClient
     * @see CogniteClient#contextualization()
     * @see Contextualization#entityMatching()
     * @see #predict(String, List, Collection, int, double)
     *
     * @param modelExternalId The external id of the matching model to use.
     * @param sources A list of entities to match from. If the list is empty, the model training sources will be used.
     * @param targets A list of entities to match to. If the list is empty, the model training targets will be used.
     * @param numMatches The maximum number of match candidates per source.
     * @return The entity matching results.
     * @throws Exception
     */
    public List<EntityMatchResult> predict(String modelExternalId,
                                           List<Struct> sources,
                                           Collection<Struct> targets,
                                           int numMatches) throws Exception {
        return predict(modelExternalId, sources, targets, numMatches, 0d);
    }

    /**
     * Matches a set of source entities with a set of targets via a given matching model.
     *
     * If either sources or targets are empty lists, the entity matcher will use
     * the sources/targets from the model training.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     String modelExternalId = // modelExternalId;
     *     List<Struct> sources = // sources ;
     *     List<Struct> targets = // targets;
     *     List<EntityMatchResult> result = client.contextualization()
     *                     .entityMatching()
     *                     .predict(modelExternalId, sources, targets, 1, 0d);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Entity-matching/operation/entityMatchingPredict">API Reference - Predict matches</a>
     *
     * @see CogniteClient
     * @see CogniteClient#contextualization()
     * @see Contextualization#entityMatching()
     *
     * @param modelExternalId The external id of the matching model to use.
     * @param sources A list of entities to match from. If the list is empty, the model training sources will be used.
     * @param targets A list of entities to match to. If the list is empty, the model training targets will be used.
     * @param numMatches The maximum number of match candidates per source.
     * @param scoreThreshold The minimum score required for a match candidate.
     * @return The entity matching results.
     * @throws Exception
     */
    public List<EntityMatchResult> predict(String modelExternalId,
                                           List<Struct> sources,
                                           Collection<Struct> targets,
                                           int numMatches,
                                           double scoreThreshold) throws Exception {
        final String loggingPrefix = "predict() - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        Preconditions.checkNotNull(modelExternalId, loggingPrefix + "Model external id cannot be null.");
        Preconditions.checkNotNull(sources, loggingPrefix + "Source cannot be null.");
        Preconditions.checkNotNull(targets, loggingPrefix + "Target cannot be null.");

        LOG.debug(loggingPrefix + "Received {} source entities to match with {} target entities.",
                sources.size(),
                targets.size());

        // Build the baseline request.
        Request request = Request.create()
                .withRootParameter("externalId", modelExternalId)
                .withRootParameter("numMatches", numMatches)
                .withRootParameter("scoreThreshold", scoreThreshold);

        // Add targets if any
        if (targets.size() > 0) {
            request = request.withRootParameter("targets", targets);
        }

        List<Request> requestBatches = new ArrayList<>();
        // Batch the source entities if any
        if (sources.size() > 0) {
            List<List<Struct>> sourceBatches =
                    Partition.ofSize(sources, getClient().getClientConfig().getEntityMatchingMaxBatchSize());
            for (List<Struct> sourceBatch : sourceBatches) {
                requestBatches.add(request.withRootParameter("sources", sourceBatch));
            }
        } else {
            requestBatches.add(request);
        }

        return predict(requestBatches);
    }

    /**
     * Matches a set of source entities with a set of targets via a given matching model.
     *
     * If either sources or targets are empty lists, the entity matcher will use
     * the sources/targets from the model training.
     *
     * The default number of matches is 1 and score threshold used for matching is 0.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Long modelId = // modelId;
     *     List<Struct> sources = // sources ;
     *     List<Struct> targets = // targets;
     *     List<EntityMatchResult> result = client.contextualization()
     *                     .entityMatching()
     *                     .predict(modelId, sources, targets);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Entity-matching/operation/entityMatchingPredict">API Reference - Predict matches</a>
     *
     * @see CogniteClient
     * @see CogniteClient#contextualization()
     * @see Contextualization#entityMatching()
     * @see #predict(long, List, Collection, int)
     *
     * @param modelId The internal id of the matching model to use.
     * @param sources A list of entities to match from. If the list is empty, the model training sources will be used.
     * @param targets A list of entities to match to. If the list is empty, the model training targets will be used.
     * @return The entity matching results.
     * @throws Exception
     */
    public List<EntityMatchResult> predict(long modelId,
                                           List<Struct> sources,
                                           Collection<Struct> targets) throws Exception {
        return predict(modelId, sources, targets, 1);
    }

    /**
     * Matches a set of source entities with a set of targets via a given matching model.
     *
     * If either sources or targets are empty lists, the entity matcher will use
     * the sources/targets from the model training.
     *
     * The default score threshold used for matching is 0.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Long modelId = // modelId;
     *     List<Struct> sources = // sources ;
     *     List<Struct> targets = // targets;
     *     List<EntityMatchResult> result = client.contextualization()
     *                     .entityMatching()
     *                     .predict(modelId, sources, targets, 1);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Entity-matching/operation/entityMatchingPredict">API Reference - Predict matches</a>
     *
     * @see CogniteClient
     * @see CogniteClient#contextualization()
     * @see Contextualization#entityMatching()
     * @see #predict(long, List, Collection, int, double)
     *
     * @param modelId The internal id of the matching model to use.
     * @param sources A list of entities to match from. If the list is empty, the model training sources will be used.
     * @param targets A list of entities to match to. If the list is empty, the model traning targets will be used.
     * @param numMatches The maximum number of match candidates per source.
     * @return The entity matching results.
     * @throws Exception
     */
    public List<EntityMatchResult> predict(long modelId,
                                           List<Struct> sources,
                                           Collection<Struct> targets,
                                           int numMatches) throws Exception {
        return predict(modelId, sources, targets, numMatches, 0d);
    }

    /**
     * Matches a set of source entities with a set of targets via a given matching model.
     *
     * If either sources or targets are empty lists, the entity matcher will use
     * the sources/targets from the model training.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Long modelId = // modelId;
     *     List<Struct> sources = // sources ;
     *     List<Struct> targets = // targets;
     *     List<EntityMatchResult> result = client.contextualization()
     *                     .entityMatching()
     *                     .predict(modelId, sources, targets, 1, 0d);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Entity-matching/operation/entityMatchingPredict">API Reference - Predict matches</a>
     *
     * @see CogniteClient
     * @see CogniteClient#contextualization()
     * @see Contextualization#entityMatching()
     *
     * @param modelId The internal id of the matching model to use.
     * @param sources A list of entities to match from. If the list is empty, the model training sources will be used.
     * @param targets A list of entities to match to. If the list is empty, the model traning targets will be used.
     * @param numMatches The maximum number of match candidates per source.
     * @param scoreThreshold The minimum score required for a match candidate.
     * @return The entity matching results.
     * @throws Exception
     */
    public List<EntityMatchResult> predict(long modelId,
                                           List<Struct> sources,
                                           Collection<Struct> targets,
                                           int numMatches,
                                           double scoreThreshold) throws Exception {
        final String loggingPrefix = "predict() - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        Preconditions.checkNotNull(sources, loggingPrefix + "Source cannot be null.");
        Preconditions.checkNotNull(targets, loggingPrefix + "Target cannot be null.");

        LOG.debug(loggingPrefix + "Received {} source entities to match with {} target entities.",
                sources.size(),
                targets.size());

        // Build the baseline request.
        Request request = Request.create()
                .withRootParameter("id", modelId)
                .withRootParameter("numMatches", numMatches)
                .withRootParameter("scoreThreshold", scoreThreshold);

        // Add targets if any
        if (targets.size() > 0) {
            request = request.withRootParameter("targets", targets);
        }

        List<Request> requestBatches = new ArrayList<>();
        // Batch the source entities if any
        if (sources.size() > 0) {
            List<List<Struct>> sourceBatches =
                    Partition.ofSize(sources, getClient().getClientConfig().getEntityMatchingMaxBatchSize());
            for (List<Struct> sourceBatch : sourceBatches) {
                requestBatches.add(request.withRootParameter("sources", sourceBatch));
            }
        } else {
            requestBatches.add(request);
        }

        return predict(requestBatches);
    }

    /**
     * Matches a set of source entities with a set of targets via a given matching model.
     *
     * If either sources or targets are empty lists, the entity matcher will use
     * the sources/targets from the model training.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<Struct> sourceBatch = // List of Struct
     *     List<Request> requestBatches = new ArrayList<>();
     *     requestBatches.add(Request.create().withRootParameter("sources", sourceBatch));
     *     List<EntityMatchResult> result = client.contextualization()
     *                     .entityMatching()
     *                     .predict(requestBatches);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Entity-matching/operation/entityMatchingPredict">API Reference - Predict matches</a>
     *
     * @see CogniteClient
     * @see CogniteClient#contextualization()
     * @see Contextualization#entityMatching()
     *
     * All input parameters are provided via the request object.
     * @param requests input parameters for the predict jobs.
     * @return The entity match results.
     * @throws Exception
     */
    public List<EntityMatchResult> predict(Collection<Request> requests) throws Exception {
        final String loggingPrefix = "predict() - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        Preconditions.checkNotNull(requests, loggingPrefix + "Requests cannot be null.");
        Instant startInstant = Instant.now();
        LOG.debug(loggingPrefix + "Received {} entity matcher requests.",
                requests.size());

        if (requests.isEmpty()) {
            LOG.info(loggingPrefix + "No items specified in the request. Will skip the predict request.");
            return Collections.emptyList();
        }

        ItemReader<String> entityMatcher = getClient().getConnectorService().entityMatcherPredict();

        List<CompletableFuture<ResponseItems<String>>> resultFutures = new ArrayList<>();
        for (Request request : requests) {
            resultFutures.add(entityMatcher.getItemsAsync(addAuthInfo(request)));
        }
        LOG.info(loggingPrefix + "Submitted {} entity matching jobs within a duration of {}.",
                requests.size(),
                Duration.between(startInstant, Instant.now()).toString());

        // Sync all downloads to a single future. It will complete when all the upstream futures have completed.
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(resultFutures.toArray(
                new CompletableFuture[resultFutures.size()]));
        // Wait until the uber future completes.
        allFutures.join();

        // Collect the response items
        List<String> responseItems = new ArrayList<>();
        for (CompletableFuture<ResponseItems<String>> responseItemsFuture : resultFutures) {
            if (!responseItemsFuture.join().isSuccessful()) {
                // something went wrong with the request
                String message = loggingPrefix + "Entity matching job failed: "
                        + responseItemsFuture.join().getResponseBodyAsString();
                LOG.error(message);
                throw new Exception(message);
            }
            responseItemsFuture.join().getResultsItems().forEach(result -> responseItems.add(result));
        }

        LOG.info(loggingPrefix + "Completed matching {} entities across {} matching jobs within a duration of {}.",
                responseItems.size(),
                requests.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return responseItems.stream()
                .map(this::parseEntityMatchResult)
                .collect(Collectors.toList());
    }


    /**
     * Train a model that predicts matches between entities (for example, time series names to asset names).
     * This is also known as fuzzy joining. If there are no trueMatches (labeled data), you train a
     * static (unsupervised) model, otherwise a machine learned (supervised) model is trained.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<Struct> sources = // sources ;
     *     List<Struct> targets = // targets;
     *     String[] modelTypes = {"simple", "insensitive", "bigram", "frequencyweightedbigram",
     *                     "bigramextratokenizers", "bigramcombo"};
     *     Request entityMatchFitRequest = Request.create()
     *                 .withRootParameter("sources",  sources)
     *                 .withRootParameter("targets", targets)
     *                 .withRootParameter("matchFields", Map.of("source", "name", "target", "externalId"))
     *                 .withRootParameter("featureType", modelTypes[1]);
     *
     *     List<EntityMatchModel> models = client.contextualization().entityMatching()
     *                 .create(List.of(entityMatchFitRequest));
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Entity-matching/operation/entityMatchingCreate">API Reference - Create entity matcher model</a>
     *
     * @see CogniteClient
     * @see CogniteClient#contextualization()
     * @see Contextualization#entityMatching()
     *
     * All input parameters are provided via the request object.
     * @param requests Input parameters for the create model job(s).
     * @return The created entity match models
     * @throws Exception
     */
    public List<EntityMatchModel> create(Collection<Request> requests) throws Exception {
        final String loggingPrefix = "create() - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        Preconditions.checkNotNull(requests, loggingPrefix + "Requests cannot be null.");
        Instant startInstant = Instant.now();
        LOG.debug(loggingPrefix + "Received {} create model requests.",
                requests.size());

        if (requests.isEmpty()) {
            LOG.info(loggingPrefix + "No items specified in the request. Will skip the create model request.");
            return Collections.emptyList();
        }

        Connector<String> entityMatcher = getClient().getConnectorService().entityMatcherFit();

        List<CompletableFuture<ResponseItems<String>>> resultFutures = new ArrayList<>();
        for (Request request : requests) {
            resultFutures.add(entityMatcher.executeAsync(addAuthInfo(request)));
        }
        LOG.info(loggingPrefix + "Submitted {} create model jobs within a duration of {}.",
                requests.size(),
                Duration.between(startInstant, Instant.now()).toString());

        // Sync all downloads to a single future. It will complete when all the upstream futures have completed.
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(resultFutures.toArray(
                new CompletableFuture[resultFutures.size()]));
        // Wait until the uber future completes.
        allFutures.join();

        // Collect the response items
        List<String> responseItems = new ArrayList<>();
        for (CompletableFuture<ResponseItems<String>> responseItemsFuture : resultFutures) {
            if (!responseItemsFuture.join().isSuccessful()) {
                // something went wrong with the request
                String message = loggingPrefix + "Create model job failed: "
                        + responseItemsFuture.join().getResponseBodyAsString();
                LOG.error(message);
                throw new Exception(message);
            }
            responseItemsFuture.join().getResultsItems().forEach(result -> responseItems.add(result));
        }

        LOG.info(loggingPrefix + "Completed creating {} models within a duration of {}.",
                requests.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return responseItems.stream()
                .map(this::parseEntityMatchModel)
                .collect(Collectors.toList());
    }

    /**
     * Deletes a set of entity matching models.
     *
     * The models to delete are identified via their {@code externalId / id} by submitting a list of
     * {@link Item}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<Item> entityMatchingModels = List.of(Item.newBuilder().setExternalId("1").build());
     *     List<Item> deleteItemsResults = client.contextualization().entityMatching()
     *                                              .delete(entityMatchingModels);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Entity-matching/operation/entityMatchingDelete">API Reference - Delete entity matcher model</a>
     *
     * @see CogniteClient
     * @see CogniteClient#contextualization()
     * @see Contextualization#entityMatching()
     * @see DeleteItems#deleteItems(List)
     *
     * @param entityMatchingModels a list of {@link Item} representing the entity matching models (externalId / id)
     *                             to be deleted
     * @return The deleted models via {@link Item}
     * @throws Exception
     */
    public List<Item> delete(List<Item> entityMatchingModels) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteEntityMatcherModels();

        DeleteItems deleteItems = DeleteItems.ofItem(deleteItemWriter, getClient().buildAuthConfig())
                //.addParameter("ignoreUnknownIds", true)
                ;

        return deleteItems.deleteItems(entityMatchingModels);
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private EntityMatchResult parseEntityMatchResult(String json) {
        try {
            return EntityMatchingParser.parseEntityMatchResult(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private EntityMatchModel parseEntityMatchModel(String json) {
        try {
            return EntityMatchingParser.parseEntityMatchModel(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract EntityMatching build();
    }
}
