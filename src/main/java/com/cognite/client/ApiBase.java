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

import com.cognite.client.config.AuthConfig;
import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.AggregateParser;
import com.cognite.client.servicesV1.parser.ItemParser;
import com.cognite.client.util.Partition;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base class for the various apis (asset, event, ts, raw, etc.).
 *
 * This class collects the set of common attributes across all apis. The individual api
 * implementations will automatically pick these up via the AutoValue generator.
 *
 * @see <a href="https://docs.cognite.com/api/v1/">Cognite API v1 specification</a>
 */
abstract class ApiBase {
    protected static final Logger LOG = LoggerFactory.getLogger(ApiBase.class);
    private static final ImmutableList<ResourceType> resourcesSupportingPartitions =
            ImmutableList.of(ResourceType.ASSET, ResourceType.EVENT, ResourceType.FILE_HEADER, ResourceType.TIMESERIES_HEADER,
                    ResourceType.RAW_ROW, ResourceType.RELATIONSHIP, ResourceType.SEQUENCE_HEADER, ResourceType.THREED_NODE);

    public abstract CogniteClient getClient();

    /**
     * Builds an array of partition specifications for parallel retrieval from the Cognite api. This specification
     * is used as a parameter together with the filter / list endpoints.
     *
     * The number of partitions indicate the number of parallel read streams. Employ one partition specification
     * per read stream.
     *
     * <h2>Example:</h2>
     *
     * <pre>
     * {@code
     *      List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());
     * }
     * </pre>
     *
     * @param noPartitions The total number of partitions
     * @return a {@link List} of partition specifications
     */
    protected List<String> buildPartitionsList(int noPartitions) {
        List<String> partitions = new ArrayList<>(noPartitions);
        for (int i = 1; i <= noPartitions; i++) {
            // Build the partitions list in the format "m/n" where m = partition no and n = total no partitions
            partitions.add(String.format("%1$d/%2$d", i, noPartitions));
        }
        return partitions;
    }

    /**
     * Will return the results from a {@code list / filter} api endpoint. For example, the {@code filter assets}
     * endpoint.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * This method support parallel retrieval via a set of {@code partition} specifications. The specified partitions
     * will be collected and merged together before being returned via the {@link Iterator}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Iterator<List<String>> result = listJson(resourceType, requestParameters, partitions);
     * }
     * </pre>
     *
     * @see #listJson(ResourceType,Request,String,String...)
     *
     * @param resourceType      The resource type to query / filter / list. Ex. {@code event, asset, time series}.
     * @param requestParameters The query / filter specification. Follows the Cognite api request parameters.
     * @param partitions        An optional set of partitions to read via.
     * @return an {@link Iterator} over the results set.
     * @throws Exception
     */
    protected Iterator<List<String>> listJson(ResourceType resourceType,
                                              Request requestParameters,
                                              String... partitions) throws Exception {
        return listJson(resourceType, requestParameters, "partition", partitions);
    }

    /**
     * Will return the results from a {@code list / filter} api endpoint. For example, the {@code filter assets}
     * endpoint.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * This method support parallel retrieval via a set of {@code partition} specifications. The specified partitions
     * will be collected and merged together before being returned via the {@link Iterator}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Iterator<List<String>> result = listJson(resourceType, requestParameters, partitionKey, partitions);
     * }
     * </pre>
     *
     * @param resourceType      The resource type to query / filter / list. Ex. {@code event, asset, time series}.
     * @param requestParameters The query / filter specification. Follows the Cognite api request parameters.
     * @param partitionKey      The key to use for the partitions in the read request. For example {@code partition}
     *                          or {@code cursor}.
     * @param partitions        An optional set of partitions to read via.
     * @return an {@link Iterator} over the results set.
     * @throws Exception
     */
    protected Iterator<List<String>> listJson(ResourceType resourceType,
                                              Request requestParameters,
                                              String partitionKey,
                                              String... partitions) throws Exception {
        // Check constraints
        if (partitions.length > 0 && !resourcesSupportingPartitions.contains(resourceType)) {
            LOG.warn(String.format("The resource type %s does not support partitions. Will ignore the partitions.",
                    resourceType));
            partitions = new String[0];
        }

        // Add auth info
        Request requestParams = addAuthInfo(requestParameters);

        // Build the api iterators.
        List<Iterator<CompletableFuture<ResponseItems<String>>>> iterators = new ArrayList<>();
        if (partitions.length < 1) {
            // No partitions specified. Just pass on the request parameters without any modification
            LOG.debug("No partitions specified. Will use a single read iterator stream.");
            iterators.add(getListResponseIterator(resourceType, requestParams));
        } else {
            // We have some partitions. Add them to the request parameters and get the iterators
            LOG.debug(String.format("Identified %d partitions. Will use a separate iterator stream per partition.",
                    partitions.length));
            for (String partition : partitions) {
                iterators.add(getListResponseIterator(resourceType,
                        requestParams.withRootParameter(partitionKey, partition)));
            }
        }

        return FanOutIterator.of(iterators);
    }

    /**
     * Retrieve items by id.
     *
     * Will ignore unknown ids by default.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Collection<Item> items = //Collection of items with ids;
     *      List<String> result = retrieveJson(resourceType, items);
     * }
     * </pre>
     *
     * @see #retrieveJson(ResourceType,Collection,Map)
     *
     * @param resourceType The item resource type ({@link com.cognite.client.dto.Event},
     *                     {@link com.cognite.client.dto.Asset}, etc.) to retrieve.
     * @param items        The item(s) {@code externalId / id} to retrieve.
     * @return The items in Json representation.
     * @throws Exception
     */
    protected List<String> retrieveJson(ResourceType resourceType, Collection<Item> items) throws Exception {
        return retrieveJson(resourceType, items, Map.of("ignoreUnknownIds", true));
    }

    /**
     * Retrieve items by id.
     *
     * This version allows you to explicitly set additional parameters for the retrieve request. For example:
     * {@code <"ignoreUnknownIds", true>} and {@code <"fetchResources", true>}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Collection<Item> items = //Collection of items with ids;
     *      Map<String, Object> parameters = //Parameters;
     *      List<String> result = retrieveJson(resourceType, items, parameters);
     * }
     * </pre>
     *
     * @param resourceType The item resource type ({@link com.cognite.client.dto.Event},
     *                     {@link com.cognite.client.dto.Asset}, etc.) to retrieve.
     * @param items        The item(s) {@code externalId / id} to retrieve.
     * @param parameters   Additional parameters for the request. For example <"ignoreUnknownIds", true>
     * @return The items in Json representation.
     * @throws Exception
     */
    protected List<String> retrieveJson(ResourceType resourceType,
                                        Collection<Item> items,
                                        Map<String, Object> parameters) throws Exception {
        String batchLogPrefix =
                "retrieveJson() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";

        // Check that ids are provided + remove duplicate ids
        Map<Long, Item> internalIdMap = new HashMap<>(items.size());
        Map<String, Item> externalIdMap = new HashMap<>(items.size());
        for (Item value : items) {
            if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                externalIdMap.put(value.getExternalId(), value);
            } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                internalIdMap.put(value.getId(), value);
            } else {
                String message = batchLogPrefix + "Item does not contain id nor externalId: " + value.toString();
                LOG.error(message);
                throw new Exception(message);
            }
        }

        List<Item> deduplicatedItems = new ArrayList<>();
        deduplicatedItems.addAll(externalIdMap.values());
        deduplicatedItems.addAll(internalIdMap.values());

        return retrieveJson(resourceType, toRequestItems(deduplicatedItems), parameters);
    }

    /**
     * Retrieve items by id.
     *
     * This version allows you to explicitly set additional parameters for the retrieve request. For example:
     * {@code <"ignoreUnknownIds", true>} and {@code <"fetchResources", true>}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Collection<Item> items = //Collection of items with ids;
     *      Map<String, Object> parameters = //Parameters;
     *      List<String> result = retrieveJson(resourceType, items, parameters);
     * }
     * </pre>
     *
     * @param resourceType The item resource type ({@link com.cognite.client.dto.Event},
     *                     {@link com.cognite.client.dto.Asset}, etc.) to retrieve.
     * @param items        The item(s) to retrieve, represented by a collection of {@code Map<String, Object>}.
     * @param parameters   Additional parameters for the request. For example <"ignoreUnknownIds", true>
     * @return The items in Json representation.
     * @throws Exception
     */
    protected List<String> retrieveJson(ResourceType resourceType,
                                        List<? extends Map<String, Object>> items,
                                        Map<String, Object> parameters) throws Exception {
        String batchLogPrefix =
                "retrieveJson() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Instant startInstant = Instant.now();
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ItemReader<String> itemReader;

        LOG.debug(batchLogPrefix + "Received {} items to read.", items.size());
        if (items.isEmpty()) {
            // Should not happen, but need to safeguard against it
            LOG.info(batchLogPrefix + "Empty input. Will skip the read process.");
            return Collections.emptyList();
        }

        // Select the appropriate item reader
        switch (resourceType) {
            case ASSET:
                itemReader = connector.readAssetsById();
                break;
            case EVENT:
                itemReader = connector.readEventsById();
                break;
            case SEQUENCE_HEADER:
                itemReader = connector.readSequencesById();
                break;
            case FILE_HEADER:
                itemReader = connector.readFilesById();
                break;
            case TIMESERIES_HEADER:
                itemReader = connector.readTsById();
                break;
            case RELATIONSHIP:
                itemReader = connector.readRelationshipsById();
                break;
            case DATA_SET:
                itemReader = connector.readDataSetsById();
                break;
            case EXTRACTION_PIPELINE:
                itemReader = connector.readExtractionPipelinesById();
                break;
            case TRANSFORMATIONS:
                itemReader = connector.readTransformationsById();
                break;
            case TRANSFORMATIONS_JOBS:
                itemReader = connector.readTransformationJobsById();
                break;
            case TRANSFORMATIONS_SCHEDULES:
                itemReader = connector.readTransformationSchedulesById();
                break;
            case SPACE:
                itemReader = connector.readSpacesById();
                break;
            default:
                LOG.error(batchLogPrefix + "Not a supported resource type: " + resourceType);
                throw new Exception(batchLogPrefix + "Not a supported resource type: " + resourceType);
        }

        Partition<? extends Map<String, Object>> itemBatches = Partition.ofSize(items, 1000);

        // Build the request template
        Request requestTemplate = addAuthInfo(Request.create());
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            requestTemplate = requestTemplate.withRootParameter(entry.getKey(), entry.getValue());
        }

        // Submit all batches
        List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
        for (List<? extends Map<String, Object>> batch : itemBatches) {
            // build initial request object
            Request request = requestTemplate
                    .withItems(batch);

            futureList.add(itemReader.getItemsAsync(request));
        }

        // Wait for all requests futures to complete
        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        allFutures.join(); // Wait for all futures to complete

        // Collect the response items
        List<String> responseItems = new ArrayList<>();
        for (CompletableFuture<ResponseItems<String>> responseItemsFuture : futureList) {
            if (!responseItemsFuture.join().isSuccessful()) {
                // something went wrong with the request
                String message = batchLogPrefix + "Error while reading the results from Cognite Data Fusion: "
                        + responseItemsFuture.join().getResponseBodyAsString();
                LOG.error(message);
                throw new Exception(message);
            }
            responseItems.addAll(responseItemsFuture.join().getResultsItems());
        }

        LOG.debug(batchLogPrefix + "Successfully retrieved {} items across {} requests within a duration of {}.",
                responseItems.size(),
                futureList.size(),
                Duration.between(startInstant, Instant.now()).toString());
        return responseItems;
    }

    /**
     * Performs an item aggregation request to Cognite Data Fusion.
     *
     * The default aggregation is a total item count based on the (optional) filters in the request. Some
     * resource types, for example {@link com.cognite.client.dto.Event}, supports multiple types of aggregation.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Aggregate aggregateResult = aggregate(resourceType,requestParameters);
     * }
     * </pre>
     *
     * @param resourceType      The resource type to perform aggregation of.
     * @param requestParameters The request containing filters.
     * @return The aggregation result.
     * @throws Exception
     * @see <a href="https://docs.cognite.com/api/v1/">Cognite API v1 specification</a>
     */
    protected Aggregate aggregate(ResourceType resourceType, Request requestParameters) throws Exception {
        String batchLogPrefix =
                "aggregate() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ItemReader<String> itemReader;
        Instant startInstant = Instant.now();

        // Add auth info
        Request requestParams = addAuthInfo(requestParameters);

        switch (resourceType) {
            case ASSET:
                itemReader = connector.readAssetsAggregates();
                break;
            case EVENT:
                itemReader = connector.readEventsAggregates();
                break;
            case SEQUENCE_HEADER:
                itemReader = connector.readSequencesAggregates();
                break;
            case FILE_HEADER:
                itemReader = connector.readFilesAggregates();
                break;
            case TIMESERIES_HEADER:
                itemReader = connector.readTsAggregates();
                break;
            case DATA_SET:
                itemReader = connector.readDataSetsAggregates();
                break;
            default:
                LOG.error(batchLogPrefix + "Not a supported resource type: " + resourceType);
                throw new Exception(batchLogPrefix + "Not a supported resource type: " + resourceType);
        }
        ResponseItems<String> responseItems = itemReader.getItems(requestParams);

        if (!responseItems.isSuccessful()) {
            // something went wrong with the request
            String message = batchLogPrefix + "Error when sending request to Cognite Data Fusion: "
                    + responseItems.getResponseBodyAsString();
            LOG.error(message);
            throw new Exception(message);
        }
        LOG.debug(batchLogPrefix + "Successfully retrieved aggregate within a duration of {}.",
                Duration.between(startInstant, Instant.now()).toString());
        return AggregateParser.parseAggregate(responseItems.getResultsItems().get(0));
    }

    /**
     * Adds the required authentication information into the request object. If the request object already have
     * complete auth info nothing will be added.
     *
     * The following authentication schemes are supported:
     * 1) API key.
     *
     * When using an api key, this service will look up the corresponding project/tenant to issue requests to.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Request requestParams = addAuthInfo(request);
     * }
     * </pre>
     *
     * @param request The request to enrich with auth information.
     * @return The request parameters with auth info added to it.
     * @throws Exception
     */
    protected Request addAuthInfo(Request request) throws Exception {
        // Check if there already is auth info.
        if (null != request.getAuthConfig()
                && null != request.getAuthConfig().getProject()) {
            return request;
        }

        return request.withAuthConfig(getClient().buildAuthConfig());
    }

    /*
    Builds a single stream iterator to page through a query to a list/filter endpoint.
     */
    protected Iterator<CompletableFuture<ResponseItems<String>>>
    getListResponseIterator(ResourceType resourceType, Request requestParameters) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();

        Iterator<CompletableFuture<ResponseItems<String>>> results;
        switch (resourceType) {
            case ASSET:
                results = connector.readAssets(requestParameters);
                break;
            case EVENT:
                results = connector.readEvents(requestParameters);
                break;
            case SEQUENCE_HEADER:
                results = connector.readSequencesHeaders(requestParameters);
                break;
            case SEQUENCE_BODY:
                results = connector.readSequencesRows(requestParameters);
                break;
            case TIMESERIES_HEADER:
                results = connector.readTsHeaders(requestParameters);
                break;
            case FILE_HEADER:
                results = connector.readFileHeaders(requestParameters);
                break;
            case DATA_SET:
                results = connector.readDataSets(requestParameters);
                break;
            case RELATIONSHIP:
                results = connector.readRelationships(requestParameters);
                break;
            case LABEL:
                results = connector.readLabels(requestParameters);
                break;
            case SECURITY_CATEGORY:
                results = connector.readSecurityCategories(requestParameters);
                break;
            case RAW_ROW:
                results = connector.readRawRows(requestParameters);
                break;
            case EXTRACTION_PIPELINE:
                results = connector.readExtractionPipelines(requestParameters);
                break;
            case EXTRACTION_PIPELINE_RUN:
                results = connector.readExtractionPipelineRuns(requestParameters);
                break;
            case THREED_MODEL:
                results = connector.readThreeDModels(requestParameters);
                break;
            case THREED_MODEL_REVISION:
                results = connector.readThreeDModelsRevisions(requestParameters);
                break;
            case THREED_NODE:
                results = connector.readThreeDNodes(requestParameters);
                break;
            case THREED_ANCESTOR_NODE:
                results = connector.readThreeDAncestorNodes(requestParameters);
                break;
            case THREED_ASSET_MAPPINGS:
                results = connector.readThreeDAssetMappings(requestParameters);
                break;
            case TRANSFORMATIONS:
                results = connector.readTransformations(requestParameters);
                break;
            case TRANSFORMATIONS_JOBS:
                results = connector.readTransformationJobs(requestParameters);
                break;
            case TRANSFORMATIONS_JOB_METRICS:
                results = connector.readTransformationJobMetrics(requestParameters);
                break;
            case TRANSFORMATIONS_SCHEDULES:
                results = connector.readTransformationSchedules(requestParameters);
                break;
            case TRANSFORMATIONS_NOTIFICATIONS:
                results = connector.readTransformationNotifications(requestParameters);
                break;
            case SPACE:
                results = connector.readSpaces(requestParameters);
                break;
            default:
                throw new Exception("Not a supported resource type: " + resourceType);
        }

        return results;
    }

    /**
     * Parses a list of item object in json representation to typed objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<String> input = //List of json;
     *      List<Item> resultList = parseItems(input);
     * }
     * </pre>
     *
     * @param input the item list in Json string representation
     * @return the parsed item objects
     * @throws Exception
     */
    protected List<Item> parseItems(List<String> input) throws Exception {
        ImmutableList.Builder<Item> listBuilder = ImmutableList.builder();
        for (String item : input) {
            listBuilder.add(ItemParser.parseItem(item));
        }
        return listBuilder.build();
    }

    /**
     * Converts a list of {@link Item} to a request object structure (that can later be parsed to Json).
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Collection<Item> itemList = //Collection of items;
     *      List<Map<String, Object>> result = toRequestItems(itemList);
     * }
     * </pre>
     *
     * @param itemList The items to parse.
     * @return The items in request item object form.
     */
    protected List<Map<String, Object>> toRequestItems(Collection<Item> itemList) {
        List<Map<String, Object>> requestItems = new ArrayList<>();
        for (Item item : itemList) {
            if (item.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                requestItems.add(ImmutableMap.of("externalId", item.getExternalId()));
            } else {
                requestItems.add(ImmutableMap.of("id", item.getId()));
            }
        }
        return requestItems;
    }

    /**
     * De-duplicates a collection of {@link Item}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Collection<Item> itemList = //Collection of items;
     *      List<Item> result = deDuplicate(itemList);
     * }
     * </pre>
     *
     * @param itemList
     * @return
     */
    protected List<Item> deDuplicate(Collection<Item> itemList) {
        Map<String, Item> idMap = new HashMap<>();
        for (Item item : itemList) {
            if (item.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                idMap.put(item.getExternalId(), item);
            } else if (item.getIdTypeCase() == Item.IdTypeCase.ID) {
                idMap.put(String.valueOf(item.getId()), item);
            } else {
                idMap.put("", item);
            }
        }
        List<Item> deDuplicated = new ArrayList<>(idMap.values());

        return deDuplicated;
    }

    /**
     * Returns true if all items contain either an externalId or id.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Collection<Item> items = //Collection of items;
     *      boolean result = itemsHaveId(items);
     * }
     * </pre>
     *
     * @param items
     * @return
     */
    protected boolean itemsHaveId(Collection<Item> items) {
        for (Item item : items) {
            if (!getItemId(item).isPresent()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Maps all items to their externalId (primary) or id (secondary). If the id function does not return any
     * identity, the item will be mapped to the empty string.
     *
     * Via the identity mapping, this function will also perform deduplication of the input items.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Collection<Item> items = //Collection of items;
     *      Map<String, Item> result = mapItemToId(items);
     * }
     * </pre>
     *
     * @param items the items to map to externalId / id.
     * @return the {@link Map} with all items mapped to externalId / id.
     */
    protected Map<String, Item> mapItemToId(Collection<Item> items) {
        Map<String, Item> resultMap = new HashMap<>((int) (items.size() * 1.35));
        for (Item item : items) {
            resultMap.put(getItemId(item).orElse(""), item);
        }
        return resultMap;
    }

    /**
     * Try parsing the specified Json path as a {@link String}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      String json = //String of json object
     *      String result = parseString(json, "name");
     * }
     * </pre>
     *
     * @param itemJson  The Json string
     * @param fieldName The Json path to parse
     * @return The Json path as a {@link String}.
     */
    protected String parseString(String itemJson, String fieldName) {
        try {
            return ItemParser.parseString(itemJson, fieldName).orElse("");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the name attribute value from a json input.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      String json = //String of json object
     *      String result = parseName(json);
     * }
     * </pre>
     *
     * @param json the json to parse
     * @return The name value
     */
    protected String parseName(String json) {
        return parseString(json, "name");
    }

    /*
    Returns the id of an Item.
     */
    private Optional<String> getItemId(Item item) {
        Optional<String> returnValue = Optional.empty();
        if (item.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
            returnValue = Optional.of(item.getExternalId());
        } else if (item.getIdTypeCase() == Item.IdTypeCase.ID) {
            returnValue = Optional.of(String.valueOf(item.getId()));
        }
        return returnValue;
    }


    /**
     * An iterator that uses multiple input iterators and combines them into a single stream / collection.
     *
     * It is used to support multiple, parallel read streams from the Cognite api and present them as a single
     * stream to the client. The backing iterators have to support async calls in order to enjoy the improved
     * performance of parallel I/O operations.
     *
     * @param <E> the element type
     */
    @AutoValue
    public abstract static class FanOutIterator<E> implements Iterator<List<E>> {
        protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

        private static <E> Builder<E> builder() {
            return new AutoValue_ApiBase_FanOutIterator.Builder<E>();
        }

        /**
         * Returns a {@link FanOutIterator} over the provided input iterators. The input iterators have to
         * support async iterations.
         *
         * @param inputIterators the input to iterate over.
         * @param <E>            the return element type
         * @return a {@link FanOutIterator}
         */
        public static <E> FanOutIterator<E> of(List<Iterator<CompletableFuture<ResponseItems<E>>>> inputIterators) {
            return FanOutIterator.<E>builder()
                    .setInputIterators(inputIterators)
                    .build();
        }

        abstract ImmutableList<Iterator<CompletableFuture<ResponseItems<E>>>> getInputIterators();

        @Override
        public boolean hasNext() {
            boolean returnValue = false;

            // If any of the input iterators have more values, return true
            for (Iterator<CompletableFuture<ResponseItems<E>>> iterator : getInputIterators()) {
                if (iterator.hasNext()) {
                    returnValue = true;
                }
            }

            return returnValue;
        }

        @Override
        public List<E> next() throws NoSuchElementException {
            if (!this.hasNext()) {
                throw new NoSuchElementException("No more elements to iterate over.");
            }
            Instant batchStartInstant = Instant.now();
            String batchLogPrefix = "next() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
            LOG.debug(String.format(batchLogPrefix + "Start reading next iteration."));

            List<E> nextBatch = new ArrayList<>(); // Container for the results batch
            // Container for the intermediary results futures
            List<CompletableFuture<ResponseItems<E>>> futures = new ArrayList<>();
            for (Iterator<CompletableFuture<ResponseItems<E>>> iterator : getInputIterators()) {
                if (iterator.hasNext()) {
                    futures.add(iterator.next());
                }
            }
            LOG.debug(String.format(batchLogPrefix + "Collected %1$d out of %2$d futures in %3$s.",
                    futures.size(),
                    getInputIterators().size(),
                    Duration.between(batchStartInstant, Instant.now()).toString()));

            // Wait for all futures to complete
            CompletableFuture<Void> allFutures = CompletableFuture
                    .allOf(futures.toArray(new CompletableFuture[futures.size()]));
            allFutures.join();

            // Collect the results. Must use for loop (not stream) for exception handling
            try {
                for (CompletableFuture<ResponseItems<E>> future : futures) {
                    for (E item : future.join().getResultsItems()) {
                        nextBatch.add(item);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            LOG.debug(String.format(batchLogPrefix + "Retrieved %1$d results items across %2$d requests in %3$s.",
                    nextBatch.size(),
                    futures.size(),
                    Duration.between(batchStartInstant, Instant.now()).toString()));

            return nextBatch;
        }

        @AutoValue.Builder
        abstract static class Builder<E> {
            abstract Builder<E> setInputIterators(List<Iterator<CompletableFuture<ResponseItems<E>>>> value);

            abstract FanOutIterator<E> build();
        }
    }

    /**
     * This {@link Iterator} takes the input from an input {@link Iterator} and maps the output to a new
     * type via a mapping {@link Function}.
     *
     * The input {@link Iterator} must provide a {@link List} as its output. I.e. it iterates over a potentially large
     * collection via a set of batches. This iterator then applies the mapping function on each individual element
     * in the {@link List}.
     *
     * @param <T> The element type of the input iterator's list.
     * @param <R> The element type of this iterator's list.
     */
    @AutoValue
    public abstract static class AdapterIterator<T, R> implements Iterator<List<R>> {

        private static <T, R> Builder<T, R> builder() {
            return new AutoValue_ApiBase_AdapterIterator.Builder<T, R>();
        }

        /**
         * Creates a new {@link AdapterIterator} translating the input {@link Iterator} elements via the
         * provided mapping {@link Function}.
         *
         * @param inputIterator   The iterator who's elements should be translated / mapped.
         * @param mappingFunction The function used for mapping the elements.
         * @param <T>             The object type of the input iterator's list.
         * @param <R>             The object type to output in this iterator's list
         * @return The iterator producing the mapped objects.
         */
        public static <T, R> AdapterIterator<T, R> of(Iterator<List<T>> inputIterator,
                                                      Function<T, R> mappingFunction) {
            return AdapterIterator.<T, R>builder()
                    .setInputIterator(inputIterator)
                    .setMappingFunction(mappingFunction)
                    .build();
        }

        abstract Iterator<List<T>> getInputIterator();

        abstract Function<T, R> getMappingFunction();

        @Override
        public boolean hasNext() {
            return getInputIterator().hasNext();
        }

        @Override
        public List<R> next() throws NoSuchElementException {
            if (!this.hasNext()) {
                throw new NoSuchElementException("No more elements to iterate over.");
            }

            return getInputIterator().next().stream()
                    .map(getMappingFunction())
                    .collect(Collectors.toList());
        }

        @AutoValue.Builder
        abstract static class Builder<T, R> {
            abstract Builder<T, R> setInputIterator(Iterator<List<T>> value);

            abstract Builder<T, R> setMappingFunction(Function<T, R> value);

            abstract AdapterIterator<T, R> build();
        }
    }

    /**
     * This class handles upsert of items to Cognite Data Fusion. It will perform upserts via
     * insert/create, update and delete requests.
     *
     * It will also split large write batches into smaller batches distributed over multiple, parallel
     * requests.
     *
     * @param <T> The object type of the objects to upsert.
     */
    @AutoValue
    public abstract static class UpsertItems<T extends Message> {
        protected static final Logger LOG = LoggerFactory.getLogger(UpsertItems.class);
        private static final int DEFAULT_MAX_BATCH_SIZE = 1000;
        private static final int maxUpsertLoopIterations = 4;

        private boolean isDefaultBatchingFunction = true;

        private static <T extends Message> Builder<T> builder() {
            return new AutoValue_ApiBase_UpsertItems.Builder<T>()
                    .setMaxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                    .setIdFunction(UpsertItems::getId)
                    .setBatchingFunction(UpsertItems::splitIntoBatches)
                    ;
        }

        /**
         * Creates a new {@link UpsertItems} that will perform upsert actions.
         *
         * @param createWriter          The item writer for create requests.
         * @param createMappingFunction A mapping function that will translate typed objects
         *                              into JSON create/insert objects.
         * @param authConfig            The authorization info for api requests.
         * @param <T>                   The object type of the objects to upsert.
         * @return the {@link UpsertItems} for upserts.
         */
        public static <T extends Message> UpsertItems<T> of(ConnectorServiceV1.ItemWriter createWriter,
                                                            Function<T, Map<String, Object>> createMappingFunction,
                                                            AuthConfig authConfig) {
            return UpsertItems.<T>builder()
                    .setCreateItemWriter(createWriter)
                    .setCreateMappingFunction(createMappingFunction)
                    .setAuthConfig(authConfig)
                    .build();
        }

        /**
         * Get id via the common dto methods {@code getExternalId} and {@code getId}.
         *
         * This is the default implementation which uses {@code Message} reflection to find the {@code externalId} or
         * {@code id} fields.
         *
         * @param item The item to interrogate for id
         * @param <T>  The object type of the item. Must be a protobuf object.
         * @return The (external)Id if found. An empty {@link Optional} if no id is found.
         */
        private static <T extends Message> Optional<String> getId(T item) {
            Optional<String> returnObject = Optional.empty();

            java.util.Map<Descriptors.FieldDescriptor, java.lang.Object> fieldMap = item.getAllFields();
            for (Descriptors.FieldDescriptor field : fieldMap.keySet()) {
                if (field.getName().equalsIgnoreCase("space")
                        && field.getType() == Descriptors.FieldDescriptor.Type.STRING) {
                    returnObject = Optional.of((String) fieldMap.get(field));
                } else if (field.getName().equalsIgnoreCase("external_id")
                        && field.getType() == Descriptors.FieldDescriptor.Type.STRING) {
                    returnObject = Optional.of((String) fieldMap.get(field));
                } else if (field.getName().equalsIgnoreCase("id")
                        && field.getType() == Descriptors.FieldDescriptor.Type.INT64) {
                    returnObject = Optional.of(String.valueOf(fieldMap.get(field)));
                }
            }

            return returnObject;
        }

        /**
         * The default batching function, splitting items into batches based on the max batch size.
         *
         * If {@code max batch size} is configured via {@link #withMaxBatchSize(int)}, then the batching function
         * will be updated as well.
         *
         * @param items the items to be split into batches.
         * @return The batches of items.
         */
        private static <T> List<List<T>> splitIntoBatches(List<T> items) {
            return Partition.ofSize(items, DEFAULT_MAX_BATCH_SIZE);
        }

        abstract Builder<T> toBuilder();

        abstract int getMaxBatchSize();
        abstract AuthConfig getAuthConfig();
        abstract Function<T, Optional<String>> getIdFunction();
        abstract Function<T, Map<String, Object>> getCreateMappingFunction();

        /**
         * Returns the function that controls the batching of items. The default batching function is based on
         * splitting the items into batches based on the {@code maxBatchSize} setting. The default max batch size is
         * 1k. If you configure the max batch size via {@link #withMaxBatchSize(int)} then this will be picked up
         * by the default batching function.
         *
         * @return The current batching function.
         */
        abstract Function<List<T>, List<List<T>>> getBatchingFunction();

        @Nullable
        abstract Function<T, Map<String, Object>> getUpdateMappingFunction();
        @Nullable
        abstract BiFunction<T, T, Map<String, Object>> getUpdateMappingBiFunction();
        @Nullable
        abstract Function<List<Item>, List<T>> getRetrieveFunction();
        @Nullable
        abstract BiFunction<T, T, Boolean> getEqualFunction();
        @Nullable
        abstract Function<T, Item> getItemMappingFunction();

        abstract ConnectorServiceV1.ItemWriter getCreateItemWriter();

        @Nullable
        abstract ConnectorServiceV1.ItemWriter getUpdateItemWriter();

        @Nullable
        abstract ConnectorServiceV1.ItemWriter getDeleteItemWriter();

        abstract ImmutableMap<String, Object> getDeleteParameters();

        /**
         * Sets the {@link com.cognite.client.servicesV1.ConnectorServiceV1.ItemWriter} for update request.
         *
         * @param updateWriter The item writer for update requests
         * @return The {@link UpsertItems} object with the configuration applied.
         */
        public UpsertItems<T> withUpdateItemWriter(ConnectorServiceV1.ItemWriter updateWriter) {
            return toBuilder().setUpdateItemWriter(updateWriter).build();
        }

        /**
         * Sets the mapping function for translating from the typed objects into JSON update objects. This function
         * determines how updates are performed--if they are replace operations or update/patch operations.
         *
         * @param function the update mapping function
         * @return The {@link UpsertItems} object with the configuration applied.
         */
        public UpsertItems<T> withUpdateMappingFunction(Function<T, Map<String, Object>> function) {
            return toBuilder().setUpdateMappingFunction(function).build();
        }

        /**
         * Sets the mapping function for translating from the typed objects into JSON update objects. This function
         * determines how updates are performed--if they are replace operations or update/patch operations.
         *
         * This version of the update mapping functions takes two input items to produce the update object:
         * - New item. The desired target state of the item.
         * - Existing item. The current state of the item in Cognite Data Fusion
         *
         * The function should analyze the difference between the desired state (new item) and current state
         * (existing item) to produce the update object.
         *
         * @param biFunction the update mapping function comparing new item with the existing item
         * @return The {@link UpsertItems} object with the configuration applied.
         */
        public UpsertItems<T> withUpdateMappingBiFunction(BiFunction<T, T, Map<String, Object>> biFunction) {
            return toBuilder().setUpdateMappingBiFunction(biFunction).build();
        }

        /**
         * Sets the {@link com.cognite.client.servicesV1.ConnectorServiceV1.ItemWriter} for delete request.
         *
         * @param deleteWriter The item writer for delete requests
         * @return The {@link UpsertItems} object with the configuration applied.
         */
        public UpsertItems<T> withDeleteItemWriter(ConnectorServiceV1.ItemWriter deleteWriter) {
            return toBuilder().setDeleteItemWriter(deleteWriter).build();
        }

        /**
         * Adds an attribute to the delete items request.
         *
         * This is typically used to add attributes such as {@code ignoreUnknownIds} and / or {@code recursive}.
         *
         * @param key   The name of the attribute.
         * @param value The value of the attribute
         * @return The {@link UpsertItems} object with the configuration applied.
         */
        public UpsertItems<T> addDeleteParameter(String key, Object value) {
            return toBuilder().addDeleteParameter(key, value).build();
        }

        /**
         * Sets the mapping function for translating from the typed objects to an {@link Item} object. This function
         * is used when deleting objects (which is done via the Item representation).
         *
         * @param function The {@link Item} mapping function.
         * @return The {@link UpsertItems} object with the configuration applied.
         */
        public UpsertItems<T> withItemMappingFunction(Function<T, Item> function) {
            return toBuilder().setItemMappingFunction(function).build();
        }

        /**
         * Sets the id function for reading the {@code externalId / id} from the input items. This function
         * is used when orchestrating upserts--in order to identify duplicates and missing items.
         *
         * If the item does not carry an {@code externalId / id}, the function must return an
         * empty {@link Optional}.
         *
         * @param function the update mapping function
         * @return The {@link UpsertItems} object with the configuration applied.
         */
        public UpsertItems<T> withIdFunction(Function<T, Optional<String>> function) {
            return toBuilder().setIdFunction(function).build();
        }

        /**
         * Sets the retrieve function for reading objects with the {@code externalId / id} from the CDF using input items.
         * This function is used when orchestrating upserts--in order to identify duplicates and missing items.
         * If the item does not carry an {@code externalId / id}, the function must return an empty {@link Optional}.
         *
         * @param function that receives objects
         * @return The {@link UpsertItems} object with the configuration applied.
         */
        public UpsertItems<T> withRetrieveFunction(Function<List<Item>, List<T>> function) {
            return toBuilder().setRetrieveFunction(function).build();
        }

        /**
         * Sets the equality function for comparing objects with the {@code externalId / id} from CDF.
         * This function is used when orchestrating upserts--in order to compare items IDs.
         *
         * @param function that compares objects
         * @return The {@link UpsertItems} object with the configuration applied.
         */
        public UpsertItems<T> withEqualFunction(BiFunction<T, T, Boolean> function) {
            return toBuilder().setEqualFunction(function).build();
        }

        /**
         * Configures the max batch size for the individual api requests. The default batch size is 1000 items.
         *
         * @param maxBatchSize The maximum batch size per api request.
         * @return The {@link UpsertItems} object with the configuration applied.
         */
        public UpsertItems<T> withMaxBatchSize(int maxBatchSize) {
            UpsertItems.Builder<T> builder = toBuilder().setMaxBatchSize(maxBatchSize);

            if (isDefaultBatchingFunction) {
                builder = builder.setBatchingFunction((List<T> items) -> Partition.ofSize(items, maxBatchSize));
            }

            return builder.build();
        }

        /**
         * Specifies a custom batching function.
         *
         * The default batching function is based on splitting the items into batches based on the number of
         * items (which can be controlled via the {@link #withMaxBatchSize(int)} parameter).
         *
         * @param function The custom batching function
         * @return The {@link UpsertItems} object with the configuration applied.
         */
        public UpsertItems<T> withBatchingFunction(Function<List<T>, List<List<T>>> function) {
            isDefaultBatchingFunction = false;
            return toBuilder().setBatchingFunction(function).build();
        }

        /**
         * Upserts a set of items via create and update.
         *
         * This function will first try to write the items as new items. In case the items already exists
         * (based on externalId or Id), the items will be updated. Effectively this results in an upsert.
         *
         * @param items The items to be upserted.
         * @return The upserted items.
         * @throws Exception
         */
        public List<String> upsertViaCreateAndUpdate(List<T> items) throws Exception {
            Instant startInstant = Instant.now();
            String batchLogPrefix =
                    "upsertViaCreateAndUpdate() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
            Preconditions.checkArgument(itemsHaveId(items),
                    batchLogPrefix + "All items must have externalId or id.");
            LOG.debug(batchLogPrefix + "Received {} items to upsert",
                    items.size());

            // Should not happen--but need to guard against empty input
            if (items.isEmpty()) {
                LOG.debug(batchLogPrefix + "Received an empty input list. Will just output an empty list.");
                return Collections.<String>emptyList();
            }

            // Insert, update, completed lists
            List<T> elementListCreate = deduplicate(items);
            List<T> elementListUpdate = new ArrayList<>(1000);
            List<String> elementListCompleted = new ArrayList<>(elementListCreate.size());
            int deduplicatedInputCount = elementListCreate.size();

            if (deduplicatedInputCount != items.size()) {
                LOG.debug(batchLogPrefix + "Identified {} duplicate items in the input. Will perform deduplication and continue upsert process",
                        items.size() - deduplicatedInputCount);
            }

            /*
            The upsert loop. If there are items left to insert or update:
            1. Insert elements
            2. If conflict, remove duplicates into the update maps
            3. Update elements
            4. If conflicts move missing items into the insert maps
            */
            ThreadLocalRandom random = ThreadLocalRandom.current();
            String exceptionMessage = "";
            boolean logToError = false;
            for (int i = 0; i < maxUpsertLoopIterations && (elementListCreate.size() + elementListUpdate.size()) > 0;
                 i++, Thread.sleep(Math.min(500L, (10L * (long) Math.exp(i)) + random.nextLong(5)))) {
                LOG.debug(batchLogPrefix + "Start upsert loop {} with {} items to create, {} items to update and "
                                + "{} completed items at duration {}",
                        i,
                        elementListCreate.size(),
                        elementListUpdate.size(),
                        elementListCompleted.size(),
                        Duration.between(startInstant, Instant.now()).toString());

                if (i == maxUpsertLoopIterations - 1) {
                    // Add the error message to std logging
                    logToError = true;
                }

                //Insert / create items
                elementListCompleted.addAll(
                        createItemsWithConflictDetection(elementListCreate, elementListUpdate, batchLogPrefix,
                                exceptionMessage, logToError)
                );

                //Update items
                elementListCompleted.addAll(
                        updateItemsWithConflictDetection(elementListUpdate, elementListCreate, batchLogPrefix,
                                startInstant, exceptionMessage, logToError)
                );
            }

            // Check if all elements completed the upsert requests
            verifyAndLogCompleteUpsertProcess(elementListCreate, elementListUpdate, elementListCompleted,
                    deduplicatedInputCount, batchLogPrefix, startInstant, exceptionMessage);

            return elementListCompleted;
        }

        /**
         * Upserts a set of items via update and create.
         *
         * This function will first try to update the items. In case the items do not exist
         * (based on externalId or Id), the items will be created. Effectively this results in an upsert.
         *
         * @param items The items to be upserted.
         * @return The upserted items.
         * @throws Exception
         */
        public List<String> upsertViaUpdateAndCreate(List<T> items) throws Exception {
            String batchLogPrefix =
                    "upsertViaUpdateAndCreate() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
            Preconditions.checkArgument(itemsHaveId(items),
                    batchLogPrefix + "All items must have externalId or id.");
            Preconditions.checkState(null != getUpdateItemWriter(),
                    "The update item writer is not configured.");
            Preconditions.checkState(null != getUpdateMappingFunction(),
                    "The update mapping function is not configured.");
            LOG.debug(String.format(batchLogPrefix + "Received %d items to upsert",
                    items.size()));
            Instant startInstant = Instant.now();

            // Should not happen--but need to guard against empty input
            if (items.isEmpty()) {
                LOG.debug(batchLogPrefix + "Received an empty input list. Will just output an empty list.");
                return Collections.<String>emptyList();
            }

            // Insert, update, completed lists
            List<T> elementListUpdate = deduplicate(items);
            List<T> elementListCreate = new ArrayList<>(items.size() / 2);
            List<String> elementListCompleted = new ArrayList<>(elementListUpdate.size());
            int deduplicatedInputCount = elementListUpdate.size();

            if (deduplicatedInputCount != items.size()) {
                LOG.debug(batchLogPrefix + "Identified {} duplicate items in the input. Will perform deduplication and continue upsert process",
                        items.size() - deduplicatedInputCount);
            }

            /*
            The upsert loop. If there are items left to insert or update:
            1. Update elements
            2. If conflicts move missing items into the insert maps
            3. Update elements
            4. If conflict, remove duplicates into the update maps
            */
            ThreadLocalRandom random = ThreadLocalRandom.current();
            String exceptionMessage = "";
            boolean logToError = false;
            for (int i = 0; i < maxUpsertLoopIterations && (elementListCreate.size() + elementListUpdate.size()) > 0;
                 i++, Thread.sleep(Math.min(500L, (10L * (long) Math.exp(i)) + random.nextLong(5)))) {
                LOG.debug(batchLogPrefix + "Start upsert loop {} with {} items to update, {} items to create and "
                                + "{} completed items at duration {}",
                        i,
                        elementListUpdate.size(),
                        elementListCreate.size(),
                        elementListCompleted.size(),
                        Duration.between(startInstant, Instant.now()).toString());

                if (i == maxUpsertLoopIterations - 1) {
                    // Add the error message to std logging
                    logToError = true;
                }

                //Update items
                elementListCompleted.addAll(
                        updateItemsWithConflictDetection(elementListUpdate, elementListCreate, batchLogPrefix,
                                startInstant, exceptionMessage, logToError)
                );

                //Insert / create items
                elementListCompleted.addAll(
                        createItemsWithConflictDetection(elementListCreate, elementListUpdate, batchLogPrefix,
                                exceptionMessage, logToError)
                );
            }

            // Check if all elements completed the upsert requests
            verifyAndLogCompleteUpsertProcess(elementListCreate, elementListUpdate, elementListCompleted,
                    deduplicatedInputCount, batchLogPrefix, startInstant, exceptionMessage);

            return elementListCompleted;
        }

        /**
         * Upserts a set of items via delete and create.
         *
         * This function will first try to delete the items (in case they already exist in CDF)
         * before creating them. Effectively this results in an upsert.
         *
         * This method is used for resource types that do not support updates natively
         * in the CDF api.
         *
         * @param items The items to be upserted.
         * @return The upserted items.
         * @throws Exception
         */
        public List<String> upsertViaDeleteAndCreate(List<T> items) throws Exception {
            String batchLogPrefix =
                    "upsertViaDeleteAndCreate() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
            Preconditions.checkState(null != getDeleteItemWriter(),
                    batchLogPrefix + "The delete item writer is not configured.");
            Preconditions.checkState(null != getItemMappingFunction(),
                    batchLogPrefix + "The item mapping function is not configured.");
            Preconditions.checkArgument(itemsHaveId(items),
                    batchLogPrefix + "All items must have externalId or id.");
            LOG.debug(String.format(batchLogPrefix + "Received %d items to upsert",
                    items.size()));

            Instant startInstant = Instant.now();

            // Should not happen--but need to guard against empty input
            if (items.isEmpty()) {
                LOG.debug(batchLogPrefix + "Received an empty input list. Will just output an empty list.");
                return Collections.<String>emptyList();
            }

            // Configure the delete handler
            DeleteItems deleteItemsHandler = DeleteItems.ofItem(getDeleteItemWriter(), getAuthConfig())
                    .setParameters(getDeleteParameters());

            // Delete, create, completed lists
            List<T> elementListCreate = deduplicate(items);
            List<T> elementListDelete = new ArrayList<>();
            List<String> elementListCompleted = new ArrayList<>();
            int deduplicatedInputCount = elementListCreate.size();

            if (deduplicatedInputCount != items.size()) {
                LOG.debug(batchLogPrefix + "Identified {} duplicate items in the input. Will perform deduplication and continue upsert process",
                        items.size() - deduplicatedInputCount);
            }

            /*
            The upsert loop. If there are items left to insert or delete:
            1. Delete elements.
            2. Copy delete elements to create list
            2. Create elements.
            3. If conflicts, move duplicates to delete elements.
            */
            ThreadLocalRandom random = ThreadLocalRandom.current();
            String exceptionMessage = "";
            boolean logToError = false;
            for (int i = 0; i < maxUpsertLoopIterations && (elementListCreate.size() + elementListDelete.size()) > 0;
                 i++, Thread.sleep(Math.min(500L, (10L * (long) Math.exp(i)) + random.nextLong(5)))) {
                LOG.debug(batchLogPrefix + "Start upsert loop {} with {} items to delete, {} items to create and "
                                + "{} completed items at duration {}",
                        i,
                        elementListDelete.size(),
                        elementListCreate.size(),
                        elementListCompleted.size(),
                        Duration.between(startInstant, Instant.now()).toString());

                if (i == maxUpsertLoopIterations - 1) {
                    // Add the error message to std logging
                    logToError = true;
                }

                /*
                Delete items
                 */
                if (elementListDelete.isEmpty()) {
                    LOG.debug(batchLogPrefix + "Delete items list is empty. Skipping delete.");
                } else {
                    // Convert input elements to Item and submit the delete requests.
                    List<Item> deleteItems = new ArrayList<>(elementListDelete.size());
                    elementListDelete.forEach(element -> deleteItems.add(getItemMappingFunction().apply(element)));
                    deleteItemsHandler.deleteItems(deleteItems);

                    // Copy the deleted elements to the create list.
                    elementListCreate.addAll(elementListDelete);
                    elementListDelete.clear();
                }

                //Insert / create items
                elementListCompleted.addAll(
                        createItemsWithConflictDetection(elementListCreate, elementListDelete, batchLogPrefix,
                                exceptionMessage, logToError)
                );
            }

            // Check if all elements completed the upsert requests
            verifyAndLogCompleteUpsertProcess(elementListCreate, elementListDelete, elementListCompleted,
                    deduplicatedInputCount, batchLogPrefix, startInstant, exceptionMessage);

            return elementListCompleted;
        }

        /**
         * Upserts a set of items via get, create and update.
         *
         * This function will first try to get all the items (in case they already exist in CDF)
         * before creating them. Items that already exists will be updated while new items will be
         * created. Effectively this results in an upsert.
         *
         * This method is used for resource types that do not support updates natively
         * in the CDF api.
         *
         * @param items The items to be upserted.
         * @return The upserted items.
         * @throws Exception
         */
        public List<String> upsertViaGetCreateAndUpdate(List<T> items) throws Exception {
            Instant startInstant = Instant.now();
            String batchLogPrefix =
                    "upsertViaGetCreateAndUpdate() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
            Preconditions.checkArgument(itemsHaveId(items),
                    batchLogPrefix + "All items must have externalId or id.");
            Preconditions.checkState(null != getItemMappingFunction(),
                    batchLogPrefix + "The item mapping function is not configured.");
            Preconditions.checkState(null != getRetrieveFunction(),
                    batchLogPrefix + "The retrieve function is not configured.");

            LOG.debug(batchLogPrefix + "Received {} items to upsert", items.size());

            // Should not happen--but need to guard against empty input
            if (items.isEmpty()) {
                LOG.debug(batchLogPrefix + "Received an empty input list. Will just output an empty list.");
                return Collections.emptyList();
            }

            // Insert, update, completed lists
            List<T> elementListGet = deduplicate(items);
            List<String> elementListCompleted = new ArrayList<>(elementListGet.size());
            int deduplicatedInputCount = elementListGet.size();

            if (deduplicatedInputCount != items.size()) {
                LOG.debug(batchLogPrefix + "Identified {} duplicate items in the input. Will deduplicate and continue with the upsert process.",
                        items.size() - deduplicatedInputCount);
            }

            final Set<T> existingResources = Set.copyOf(Objects.requireNonNull(getRetrieveFunction())
                    .apply(elementListGet.stream()
                            .map(item -> Objects.requireNonNull(getItemMappingFunction()).apply(item))
                            .collect(Collectors.toList())));


            List<T> elementListCreate = new ArrayList<>(1000);
            List<T> elementListUpdate = new ArrayList<>(1000);

            elementListGet.forEach(item -> {
                if (existingResources.stream()
                        .anyMatch(existing -> Objects.requireNonNull(getEqualFunction()).apply(existing, item))) {
                    elementListUpdate.add(item);
                } else {
                    elementListCreate.add(item);
                }
            });

            /*
            The upsert loop. If there are items left to insert or update:
            1. Insert elements (that were not found)
            2. If a conflict, remove duplicates into the update maps
            3. Update elements (that were found)
            4. If conflicts move missing items into the insert maps
            */
            ThreadLocalRandom random = ThreadLocalRandom.current();
            String exceptionMessage = "";
            boolean logToError = false;
            for (int i = 0; i < maxUpsertLoopIterations && (elementListCreate.size() + elementListUpdate.size()) > 0;
                 i++, Thread.sleep(Math.min(500L, (10L * (long) Math.exp(i)) + random.nextLong(5)))) {
                LOG.debug(batchLogPrefix + "Start upsert loop {} with {} items to create, {} items to update and "
                                + "{} completed items at duration {}",
                        i,
                        elementListCreate.size(),
                        elementListUpdate.size(),
                        elementListCompleted.size(),
                        Duration.between(startInstant, Instant.now()).toString());

                if (i == maxUpsertLoopIterations - 1) {
                    // Add the error message to std logging
                    logToError = true;
                }

                //Insert / create items
                elementListCompleted.addAll(
                        createItemsWithConflictDetection(elementListCreate, elementListUpdate, batchLogPrefix,
                                exceptionMessage, logToError)
                );

                //Update items
                elementListCompleted.addAll(
                        updateItemsWithConflictDetection(elementListUpdate, elementListCreate, batchLogPrefix,
                                startInstant, exceptionMessage, logToError)
                );
            }

            // Check if all elements completed the upsert requests
            verifyAndLogCompleteUpsertProcess(elementListCreate, elementListUpdate, elementListCompleted,
                    deduplicatedInputCount, batchLogPrefix, startInstant, exceptionMessage);

            return elementListCompleted;
        }

        /**
         * Upserts a set of items via get, create and update, with change detection for the updates.
         *
         * This function will first try to get all the items (in case they already exist in CDF)
         * before creating them. New items (that does not exist in CDF) will be created. Existing items will be updated
         * via an update procedure that compares the existing CDF item with the new (input) item in order to produce
         * the update request.
         *
         * This method is used for resource types that do not support updates natively
         * in the CDF api.
         *
         * @param items The items to be upserted.
         * @return The upserted items.
         * @throws Exception
         */
        public List<String> upsertViaGetCreateAndUpdateDiff(List<T> items) throws Exception {
            Instant startInstant = Instant.now();
            String batchLogPrefix =
                    "upsertViaGetCreateAndUpdateDiff() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
            Preconditions.checkArgument(itemsHaveId(items),
                    batchLogPrefix + "All items must have externalId or id.");
            Preconditions.checkState(null != getItemMappingFunction(),
                    batchLogPrefix + "The item mapping function is not configured.");
            Preconditions.checkState(null != getUpdateMappingBiFunction(),
                    batchLogPrefix + "The update mapping bi-function is not configured.");
            Preconditions.checkState(null != getRetrieveFunction(),
                    batchLogPrefix + "The retrieve function is not configured.");
            Preconditions.checkState(null != getEqualFunction(),
                    batchLogPrefix + "The equal function is not configured.");

            LOG.debug(batchLogPrefix + "Received {} items to upsert", items.size());

            // Should not happen--but need to guard against empty input
            if (items.isEmpty()) {
                LOG.debug(batchLogPrefix + "Received an empty input list. Will just output an empty list.");
                return Collections.emptyList();
            }

            // Insert, update, completed lists
            List<T> elementListGet = deduplicate(items);
            List<String> elementListCompleted = new ArrayList<>(elementListGet.size());
            int deduplicatedInputCount = elementListGet.size();

            if (deduplicatedInputCount != items.size()) {
                LOG.debug(batchLogPrefix + "Identified {} duplicate items in the input. Will deduplicate and continue with the upsert process.",
                        items.size() - deduplicatedInputCount);
            }

            final List<T> existingResources = List.copyOf(Objects.requireNonNull(getRetrieveFunction())
                    .apply(elementListGet.stream()
                            .map(item -> Objects.requireNonNull(getItemMappingFunction()).apply(item))
                            .collect(Collectors.toList())));


            List<T> elementListCreate = new ArrayList<>();
            List<T> elementListUpdate = new ArrayList<>();

            elementListGet.forEach(item -> {
                if (existingResources.stream()
                        .anyMatch(existing -> Objects.requireNonNull(getEqualFunction()).apply(existing, item))) {
                    elementListUpdate.add(item);
                } else {
                    elementListCreate.add(item);
                }
            });

            /*
            The upsert loop. If there are items left to insert or update:
            1. Insert elements (that were not found)
            2. If a conflict, remove duplicates into the update maps
            3. Update elements (that were found)
            4. If conflicts move missing items into the insert maps
            */
            ThreadLocalRandom random = ThreadLocalRandom.current();
            String exceptionMessage = "";
            boolean logToError = false;
            for (int i = 0; i < maxUpsertLoopIterations && (elementListCreate.size() + elementListUpdate.size()) > 0;
                 i++, Thread.sleep(Math.min(500L, (10L * (long) Math.exp(i)) + random.nextLong(5)))) {
                LOG.debug(batchLogPrefix + "Start upsert loop {} with {} items to create, {} items to update and "
                                + "{} completed items at duration {}",
                        i,
                        elementListCreate.size(),
                        elementListUpdate.size(),
                        elementListCompleted.size(),
                        Duration.between(startInstant, Instant.now()).toString());

                if (i == maxUpsertLoopIterations - 1) {
                    // Add the error message to std logging
                    logToError = true;
                }

                //Insert / create items
                elementListCompleted.addAll(
                        createItemsWithConflictDetection(elementListCreate, elementListUpdate, batchLogPrefix,
                                exceptionMessage, logToError)
                );

                //Update items
                elementListCompleted.addAll(
                        updateItemsWithConflictDetection(elementListUpdate, existingResources, elementListCreate, batchLogPrefix,
                                startInstant, exceptionMessage, logToError)
                );
            }

            // Check if all elements completed the upsert requests
            verifyAndLogCompleteUpsertProcess(elementListCreate, elementListUpdate, elementListCompleted,
                    deduplicatedInputCount, batchLogPrefix, startInstant, exceptionMessage);

            return elementListCompleted;
        }

        /**
         * Upserts a set of items via create.
         *
         * This function will first try to write the items as new items.
         *
         * @param items The items to be created.
         * @return The created items.
         * @throws Exception
         */
        public List<String> create(List<T> items) throws Exception {
            Instant startInstant = Instant.now();
            String batchLogPrefix =
                    "create() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
            /*
            In the case of Security Categories this checks if the items have a name, as security categories do not have
            external ids, and the id is assigned by the api on creation and accepted as input the api.
             */
            Preconditions.checkArgument(itemsHaveId(items),
                    batchLogPrefix + "All items must have externalId or id.");

            // Should not happen--but need to guard against empty input
            if (items.isEmpty()) {
                LOG.debug(batchLogPrefix + "Received an empty input list. Will just output an empty list.");
                return Collections.<String>emptyList();
            }

            // Insert, completed lists
            List<T> elementListCreate = deduplicate(items);
            List<String> elementListCompleted = new ArrayList<>(elementListCreate.size());
            int deduplicatedInputCount = elementListCreate.size();

            if (deduplicatedInputCount != items.size()) {
                LOG.debug(batchLogPrefix + "Identified {} duplicate items in the input. Will perform deduplication and continue upsert process",
                        items.size() - deduplicatedInputCount);
            }

            /*
            The upsert loop. If there are items left to insert:
            1. Insert elements
            2. If conflict, report to client
            */
            String exceptionMessage = "";
            LOG.debug(batchLogPrefix + "Start create loop with {} items to create and "
                            + "{} completed items at duration {}",
                    elementListCreate.size(),
                    elementListCompleted.size(),
                    Duration.between(startInstant, Instant.now()).toString());

            //Insert / create items
            elementListCompleted.addAll(
                    createItemsWithConflictDetection(elementListCreate, new ArrayList<T>(), batchLogPrefix,
                            exceptionMessage, true)
            );

            // Check if all elements completed the upsert requests
            verifyAndLogCompleteUpsertProcess(elementListCreate, new ArrayList<T>(), elementListCompleted,
                    deduplicatedInputCount, batchLogPrefix, startInstant, exceptionMessage);

            return elementListCompleted;
        }

        /**
         * Upserts a set of items via create and delete.
         *
         * This function will first try to create the items, in case the items already exists
         * (based on externalId or Id) the items will be deleted and created again.
         * Effectively this results in an upsert.
         *
         * This method is used for resource types that do not support updates natively
         * in the CDF api and that do not have the ability to ignore unknown ids.
         *
         * @param items The items to be upserted.
         * @return The upserted items.
         * @throws Exception
         */
        public List<String> upsertViaCreateAndDelete(List<T> items) throws Exception {
            String batchLogPrefix =
                    "upsertViaDeleteAndCreate() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
            Preconditions.checkState(null != getDeleteItemWriter(),
                    batchLogPrefix + "The delete item writer is not configured.");
            Preconditions.checkState(null != getItemMappingFunction(),
                    batchLogPrefix + "The item mapping function is not configured.");
            Preconditions.checkArgument(itemsHaveId(items),
                    batchLogPrefix + "All items must have externalId or id.");
            LOG.debug(String.format(batchLogPrefix + "Received %d items to upsert",
                    items.size()));

            Instant startInstant = Instant.now();

            // Should not happen--but need to guard against empty input
            if (items.isEmpty()) {
                LOG.debug(batchLogPrefix + "Received an empty input list. Will just output an empty list.");
                return Collections.<String>emptyList();
            }

            // Configure the delete handler
            DeleteItems deleteItemsHandler = DeleteItems.ofItem(getDeleteItemWriter(), getAuthConfig())
                    .setParameters(getDeleteParameters());

            // Insert, update, completed lists
            List<T> elementListCreate = deduplicate(items);
            List<T> elementListDelete = new ArrayList<>();
            List<String> elementListCompleted = new ArrayList<>(elementListCreate.size());
            int deduplicatedInputCount = elementListCreate.size();

            if (deduplicatedInputCount != items.size()) {
                LOG.debug(batchLogPrefix + "Identified {} duplicate items in the input. Will perform deduplication and continue upsert process",
                        items.size() - deduplicatedInputCount);
            }

            /*
            The upsert loop. If there are items left to insert or delete:
            1. Create elements.
                a. If conflict, move duplicates to the delete list.
            2. Delete elements.
            3. Move delete items to create (for re-try)
            */
            ThreadLocalRandom random = ThreadLocalRandom.current();
            String exceptionMessage = "";
            boolean logToError = false;
            for (int i = 0; i < maxUpsertLoopIterations && (elementListCreate.size() + elementListDelete.size()) > 0;
                 i++, Thread.sleep(Math.min(500L, (10L * (long) Math.exp(i)) + random.nextLong(5)))) {
                LOG.debug(batchLogPrefix + "Start upsert loop {} with {} items to delete, {} items to create and "
                                + "{} completed items at duration {}",
                        i,
                        elementListDelete.size(),
                        elementListCreate.size(),
                        elementListCompleted.size(),
                        Duration.between(startInstant, Instant.now()).toString());

                if (i == maxUpsertLoopIterations - 1) {
                    // Add the error message to std logging
                    logToError = true;
                }

                //Insert / create items
                elementListCompleted.addAll(
                        createItemsWithConflictDetection(elementListCreate, elementListDelete, batchLogPrefix,
                                exceptionMessage, logToError)
                );

                /*
                Delete items
                 */
                if (elementListDelete.isEmpty()) {
                    LOG.debug(batchLogPrefix + "Delete items list is empty. Skipping delete.");
                } else {
                    // Convert input elements to Item and submit the delete requests.
                    List<Item> deleteItems = new ArrayList<>(elementListDelete.size());
                    elementListDelete.forEach(element -> deleteItems.add(getItemMappingFunction().apply(element)));
                    deleteItemsHandler.deleteItems(deleteItems);

                    // Copy the deleted elements to the create list for re-try on the next loop.
                    elementListCreate.addAll(elementListDelete);
                    elementListDelete.clear();
                }
            }

            // Check if all elements completed the upsert requests
            verifyAndLogCompleteUpsertProcess(elementListCreate, elementListDelete, elementListCompleted,
                    deduplicatedInputCount, batchLogPrefix, startInstant, exceptionMessage);

            return elementListCompleted;
        }

        /**
         * Performs an insert/create items operation on a (large) batch of items with conflict/duplicate detection.
         *
         * Large input batches will be split into smaller batches and submitted in parallel to CDF. Each insert/create
         * request/response is investigated for conflicts (currently, {@code duplicates} conflicts will be detected)
         * and reported back.
         *
         * In case of a failed request batch, we need to identify the conflicting items from the non-conflicting ones.
         * Because CDF will treat an entire request batch atomically, all items in the batch will be
         * rejected even if just a single item represents a conflict. This leads to a rather complex result being returned
         * from this method:
         * - Items that have been successfully created/inserted to CDF are returned in a {@code List<String>, json string}
         * as the {@code return object}.
         * - Items that have an insert conflict will be added to the {@code conflictItems} list. These items should be
         * re-tried as an {@code update} request.
         * - Items that does not have an insert conflict, but have not been committed to created/inserted (because they were in a
         * failed request batch) will be added to the {@code createItems} list. These items can be re-tried as an
         * insert request.
         *
         * Please note that the {@code createItems} list serves two purposes:
         * 1. When invoking this method, the list holds all items that should be created/inserted.
         * 2. When the method is finished, the lists holds the items (if any) that can be re-tried as create/insert.
         *
         * Algorithm:
         * 1. All items to create/insert are submitted in the {@code createItems} list.
         * 2. Split large input into smaller request batches (if necessary) and post to CDF.
         * 3. Clear {@code createItems}.
         * 4. For each request batch:
         *  - If successful: register all items as {@code completed}.
         *  - If unsuccessful:
         *      - Register error/conflict message in {@code exceptionMessage}.
         *      - Add duplicates to the {@code conflictItems} list.
         *      - Add non-conflicting items back to the {@code createItems} list.
         *
         * @param createItems The items to create/insert.
         * @param conflictItems The items that caused a conflict when inserting/creating.
         * @param loggingPrefix A prefix prepended to the logs.
         * @param exceptionMessage The exception message from the create/insert operation (if a conflict is detected) will be added here.
         * @param logExceptionAsError If set to {@code true}, any error message from the create/insert operation will be logged
         *                       as {@code error}. If set to {@code false}, any error will be logged as {@code debug}.
         * @return The results from the items that successfully completed the create/insert operation.
         * @throws Exception If an error is encountered during the create operation. For example, a network error.
         */
        private List<String> createItemsWithConflictDetection(List<T> createItems,
                                                              List<T> conflictItems,
                                                              String loggingPrefix,
                                                              String exceptionMessage,
                                                              boolean logExceptionAsError) throws Exception {
            List<String> completedItems = new ArrayList<>();

            if (createItems.isEmpty()) {
                LOG.debug(loggingPrefix + "Create items list is empty. Skipping create.");
            } else {
                Map<ResponseItems<String>, List<T>> createResponseMap = splitAndCreateItems(loggingPrefix, createItems);
                createItems.clear(); // Must prepare the list for possible new entries.

                for (ResponseItems<String> response : createResponseMap.keySet()) {
                    if (response.isSuccessful()) {
                        completedItems.addAll(response.getResultsItems());
                        LOG.debug(loggingPrefix + "Create items request completed successfully. Adding {} create result items to result collection.",
                                response.getResultsItems().size());
                    } else {
                        exceptionMessage = response.getResponseBodyAsString();
                        String logMessage = String.format("Create items request failed: %s", exceptionMessage);
                        if (logExceptionAsError) {
                            // Add the error message to std logging
                            LOG.error(logMessage);
                        } else {
                            LOG.debug(logMessage);
                        }
                        LOG.debug(loggingPrefix + "Converting duplicates for conflict resolution and retrying the request");
                        List<Item> duplicates = ItemParser.parseItems(response.getDuplicateItems());
                        LOG.debug(loggingPrefix + "Number of duplicate entries reported by CDF: {}", duplicates.size());

                        // Move duplicates from insert to the conflict list
                        distributeObjectsToMainAndReminder(createResponseMap.get(response),
                                duplicates,
                                conflictItems,
                                createItems);
                    }
                }
            }

            return completedItems;
        }

        /**
         * Performs an update items operation on a (large) batch of items with conflict/missing items detection.
         *
         * Large input batches will be split into smaller batches and submitted in parallel to CDF. Each update
         * request/response is investigated for conflicts (currently, {@code missing items} conflicts will be detected)
         * and reported back.
         *
         * In case of a failed request batch, we need to identify the conflicting items from the non-conflicting ones.
         * Because CDF will treat an entire request batch atomically, all items in the batch will be
         * rejected even if just a single item represents a conflict. This leads to a rather complex result being returned
         * from this method:
         * - Items that have been successfully created/inserted to CDF are returned in a {@code List<String>} as the
         * {@code return object}.
         * - Items that have an update conflict will be added to the {@code conflictItems} list. These items should be
         * re-tried as a {@code create/insert} request.
         * - Items that does not have an update conflict, but have not been committed to update (because they were in a
         * failed request batch) will be added to the {@code updateItems} list. These items can be re-tried as an
         * update request.
         *
         * Please note that the {@code updateItems} list serves two purposes:
         * 1. When invoking this method, the list holds all items that should be updated.
         * 2. When the method is finished, the lists holds the items (if any) that can be re-tried as update.
         *
         * Algorithm:
         * 1. All items to update are submitted in the {@code updateItems} list.
         * 2. Split large input into smaller request batches (if necessary) and post to CDF.
         * 3. Clear {@code updateItems}.
         * 4. For each request batch:
         *  - If successful: register all items as {@code completed}.
         *  - If unsuccessful:
         *      - Register error/conflict message in {@code exceptionMessage}.
         *      - Add missing items to the {@code conflictItems} list.
         *      - Add non-conflicting items back to the {@code updateItems} list.
         *
         * @param updateItems The items to update.
         * @param conflictItems The items that caused a conflict when inserting/creating.
         * @param loggingPrefix A prefix prepended to the logs.
         * @param startInstant The Instant when the upsert operation started. Used to calculate operation duration.
         * @param exceptionMessage The exception message from the update operation (if a conflict is detected) will be added here.
         * @param logExceptionAsError If set to {@code true}, any error message from the create/insert operation will be logged
         *                       as {@code error}. If set to {@code false}, any error will be logged as {@code debug}.
         * @return The results from the items that successfully completed the update operation.
         * @throws Exception If an error is encountered during the update operation. For example, a network error.
         */
        private List<String> updateItemsWithConflictDetection(List<T> updateItems,
                                                              List<T> conflictItems,
                                                              String loggingPrefix,
                                                              Instant startInstant,
                                                              String exceptionMessage,
                                                              boolean logExceptionAsError) throws Exception {
            Preconditions.checkState(null != getUpdateItemWriter(),
                    "The update item writer is not configured.");
            Preconditions.checkState(null != getUpdateMappingFunction(),
                    "The update mapping function is not configured.");

            List<String> completedItems = new ArrayList<>();

            if (updateItems.isEmpty()) {
                LOG.debug(loggingPrefix + "Update items list is empty. Skipping update.");
            } else {
                Map<ResponseItems<String>, List<T>> updateResponseMap = splitAndUpdateItems(loggingPrefix, updateItems);
                updateItems.clear(); // Must prepare the list for possible new entries.

                for (ResponseItems<String> response : updateResponseMap.keySet()) {
                    if (response.isSuccessful()) {
                        completedItems.addAll(response.getResultsItems());
                        LOG.debug(loggingPrefix + "Update items request completed successfully. Adding {} update result items to result collection.",
                                response.getResultsItems().size());
                    } else {
                        exceptionMessage = response.getResponseBodyAsString();
                        String logMessage = String.format("Update items request failed: %s", exceptionMessage);
                        if (logExceptionAsError) {
                            // Add the error message to std logging
                            LOG.error(logMessage);
                        } else {
                            LOG.debug(logMessage);
                        }
                        LOG.debug(loggingPrefix + "Converting missing items for conflict resolution and retrying the request");
                        List<Item> missing = ItemParser.parseItems(response.getMissingItems());
                        LOG.debug(loggingPrefix + "Number of missing entries reported by CDF: {}", missing.size());

                        // Move missing items from insert to the conflict list
                        distributeObjectsToMainAndReminder(updateResponseMap.get(response),
                                missing,
                                conflictItems,
                                updateItems);
                    }
                }
            }

            return completedItems;
        }

        /**
         * Performs an update items operation on a (large) batch of items with conflict/missing items detection. The
         * update operation is based on comparing the existing state of the items (in CDF) with the target state (the update).
         *
         * Large input batches will be split into smaller batches and submitted in parallel to CDF. Each update
         * request/response is investigated for conflicts (currently, {@code missing items} conflicts will be detected)
         * and reported back.
         *
         * In case of a failed request batch, we need to identify the conflicting items from the non-conflicting ones.
         * Because CDF will treat an entire request batch atomically, all items in the batch will be
         * rejected even if just a single item represents a conflict. This leads to a rather complex result being returned
         * from this method:
         * - Items that have been successfully created/inserted to CDF are returned in a {@code List<String>} as the
         * {@code return object}.
         * - Items that have an update conflict will be added to the {@code conflictItems} list. These items should be
         * re-tried as a {@code create/insert} request.
         * - Items that does not have an update conflict, but have not been committed to update (because they were in a
         * failed request batch) will be added to the {@code updateItems} list. These items can be re-tried as an
         * update request.
         *
         * Please note that the {@code updateItems} list serves two purposes:
         * 1. When invoking this method, the list holds all items that should be updated.
         * 2. When the method is finished, the lists holds the items (if any) that can be re-tried as update.
         *
         * Algorithm:
         * 1. All items to update are submitted in the {@code updateItems} list.
         * 2. Split large input into smaller request batches (if necessary) and post to CDF.
         * 3. Clear {@code updateItems}.
         * 4. For each request batch:
         *  - If successful: register all items as {@code completed}.
         *  - If unsuccessful:
         *      - Register error/conflict message in {@code exceptionMessage}.
         *      - Add missing items to the {@code conflictItems} list.
         *      - Add non-conflicting items back to the {@code updateItems} list.
         *
         * @param updateItems The items to update, their target state.
         * @param existingItems The items to update, their existing state (in CDF).
         * @param conflictItems The items that caused a conflict when inserting/creating.
         * @param loggingPrefix A prefix prepended to the logs.
         * @param startInstant The Instant when the upsert operation started. Used to calculate operation duration.
         * @param exceptionMessage The exception message from the update operation (if a conflict is detected) will be added here.
         * @param logExceptionAsError If set to {@code true}, any error message from the create/insert operation will be logged
         *                       as {@code error}. If set to {@code false}, any error will be logged as {@code debug}.
         * @return The results from the items that successfully completed the update operation.
         * @throws Exception If an error is encountered during the update operation. For example, a network error.
         */
        private List<String> updateItemsWithConflictDetection(List<T> updateItems,
                                                              List<T> existingItems,
                                                              List<T> conflictItems,
                                                              String loggingPrefix,
                                                              Instant startInstant,
                                                              String exceptionMessage,
                                                              boolean logExceptionAsError) throws Exception {
            Preconditions.checkState(null != getUpdateItemWriter(),
                    "The update item writer is not configured.");

            List<String> completedItems = new ArrayList<>();

            if (updateItems.isEmpty()) {
                LOG.debug(loggingPrefix + "Update items list is empty. Skipping update.");
            } else {
                Map<ResponseItems<String>, List<T>> updateResponseMap = splitAndUpdateItems(loggingPrefix, updateItems, existingItems);
                updateItems.clear(); // Must prepare the list for possible new entries.

                for (ResponseItems<String> response : updateResponseMap.keySet()) {
                    if (response.isSuccessful()) {
                        completedItems.addAll(response.getResultsItems());
                        LOG.debug(loggingPrefix + "Update items request completed successfully. Adding {} update result items to result collection.",
                                response.getResultsItems().size());
                    } else {
                        exceptionMessage = response.getResponseBodyAsString();
                        String logMessage = String.format("Update items request failed: %s", exceptionMessage);
                        if (logExceptionAsError) {
                            // Add the error message to std logging
                            LOG.error(logMessage);
                        } else {
                            LOG.debug(logMessage);
                        }
                        LOG.debug(loggingPrefix + "Converting missing items for conflict resolution and retrying the request");
                        List<Item> missing = ItemParser.parseItems(response.getMissingItems());
                        LOG.debug(loggingPrefix + "Number of missing entries reported by CDF: {}", missing.size());

                        // Move missing items from insert to the conflict list
                        distributeObjectsToMainAndReminder(updateResponseMap.get(response),
                                missing,
                                conflictItems,
                                updateItems);
                    }
                }
            }

            return completedItems;
        }

        /**
         * Distributes a set of objects from an input list to two output lists: {@code main} and {@code reminder}.
         *
         * The objects are distributed based on a second input list of {@link Item}. The {@link Item} list specifies
         * which objects to add to {@code main}. The rest are added to {@code reminder}.
         *
         * This method is used by the upsert methods when moving upsert objects between {@code create}, {@code update}
         * and {@code delete} lists.
         *
         * @param inputObjects The input objects to distribute.
         * @param itemsToMain Specifies the objects (via {@code externalId / id}) to add to {@code main}.
         * @param main The {@code main} output list.
         * @param reminder The {@code reminder} output list.
         */
        private void distributeObjectsToMainAndReminder(List<T> inputObjects,
                                                        List<Item> itemsToMain,
                                                        List<T> main,
                                                        List<T> reminder) {
            // Must check that itemsToMain refers to valid objects from inputObjects as some
            // items may refer to invalid references (from parent asset references reported as missing items).
            Map<String, T> itemsMap = mapToId(inputObjects);
            for (Item value : itemsToMain) {
                if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID
                        && itemsMap.containsKey(value.getExternalId())) {
                    main.add(itemsMap.get(value.getExternalId()));
                    itemsMap.remove(value.getExternalId());
                } else if (value.getIdTypeCase() == Item.IdTypeCase.ID
                        && itemsMap.containsKey(String.valueOf(value.getId()))) {
                    main.add(itemsMap.get(String.valueOf(value.getId())));
                    itemsMap.remove(String.valueOf(value.getId()));
                } else if (value.getIdTypeCase() == Item.IdTypeCase.LEGACY_NAME
                        && itemsMap.containsKey(value.getLegacyName())) {
                    // Special case for v1 TS headers.
                    main.add(itemsMap.get(value.getLegacyName()));
                    itemsMap.remove(value.getLegacyName());
                }
            }
            reminder.addAll(itemsMap.values()); // Add remaining items
        }

        /**
         * Checks if the input lists {@code A and B} are empty--which signals a completed upsert operation--and logs
         * the main statistics to info.
         *
         * If the input lists are not empty, then the main statistics are logged as an error and an exception is thrown.
         *
         * This method is used by the various {@code upsert} methods to verify a completed/successful upsert operation.
         *
         * @param inputListA Input list A. Must be empty for the upsert operation to be considered complete.
         * @param inputListB Input list B. Must be empty for the upsert operation to be considered complete.
         * @param outputList Output/results list. Contains the results of the completed upsert operations.
         * @param expectedOutputCount The expected number of results/output. The output list size will be compared to this
         *                            number and a warning logged if they differ.
         * @param loggingPrefix A prefix prepended to the logs.
         * @param startInstant The Instant when the upsert operation started. Used to calculate operation duration.
         * @param errorMessage An error message (from the upsert operation). Will be appended to the error log and Exception.
         * @throws Exception If the input lists are not empty.
         */
        private void verifyAndLogCompleteUpsertProcess(List<?> inputListA,
                                                       List<?> inputListB,
                                                       List<?> outputList,
                                                       int expectedOutputCount,
                                                       String loggingPrefix,
                                                       Instant startInstant,
                                                       String errorMessage) throws Exception {
            // Check if all elements completed the upsert requests
            if (inputListA.isEmpty() && inputListB.isEmpty()) {
                LOG.debug(loggingPrefix + "Successfully upserted {} items within a duration of {}.",
                        outputList.size(),
                        Duration.between(startInstant, Instant.now()).toString());
            } else {
                String message = String.format(loggingPrefix + "Failed to upsert items. %d items remaining. "
                                + " %d items completed upsert. %n %s",
                        inputListA.size() + inputListB.size(),
                        outputList.size(),
                        errorMessage);
                LOG.error(message);
                throw new Exception(message);
            }

            // Check if we have the expected number of results. This test will only be executed in case the
            // input lists are empty (seemingly successful upsert).
            if (expectedOutputCount != outputList.size()) {
                LOG.warn("The output count is different from the input count. De-duplicated input count: {}. Output count: {}",
                        expectedOutputCount,
                        outputList.size());
            }
        }

        /**
         * Create /insert items.
         *
         * Submits a (large) batch of items by splitting it up into multiple, parallel create / insert requests.
         * The response from each request is returned along with the items used as input.
         *
         * @param items the objects to create/insert.
         * @return a {@link Map} with the responses and request inputs.
         * @throws Exception
         */
        private Map<ResponseItems<String>, List<T>> splitAndCreateItems(String loggingPrefix, List<T> items) {
            return splitAndProcessItems(loggingPrefix, items, this::createItems);
        }

        /**
         * Update items.
         *
         * Submits a (large) batch of items by splitting it up into multiple, parallel update requests.
         * The response from each request is returned along with the items used as input.
         *
         * @param items the objects to create/insert.
         * @return a {@link Map} with the responses and request inputs.
         * @throws Exception
         */
        private Map<ResponseItems<String>, List<T>> splitAndUpdateItems(String loggingPrefix, List<T> items) {
            return splitAndProcessItems(loggingPrefix, items, this::updateItems);
        }

        /**
         * Update items, with diff logic. In order to build the update request (to CDF), both the current state of
         * the items and the target state are used as input. This is in contrast to {@link #splitAndUpdateItems(String, List)}
         * which only uses the target state as input.
         *
         * Submits a (large) batch of items by splitting it up into multiple, parallel update requests.
         * The response from each request is returned along with the items used as input.
         *
         * @param updateItems The items to update, their target state.
         * @param existingItems The items to update, their existing state (in CDF).
         * @return a {@link Map} with the responses and request inputs.
         * @throws Exception
         */
        private Map<ResponseItems<String>, List<T>> splitAndUpdateItems(String loggingPrefix,
                                                                        List<T> updateItems,
                                                                        List<T> existingItems) {
            Instant startInstant = Instant.now();
            Map<CompletableFuture<ResponseItems<String>>, List<T>> responseMap = new HashMap<>();

            // Split into batches
            List<List<T>> updateItemBatches = getBatchingFunction().apply(updateItems);

            // Submit all batches
            for (List<T> batch : updateItemBatches) {
                responseMap.put(updateItems(batch, existingItems), batch);
            }
            LOG.debug(loggingPrefix + "Completed building requests for {} items across {} batches at duration {}",
                    updateItems.size(),
                    responseMap.size(),
                    Duration.between(startInstant, Instant.now()).toString());

            // Wait for all requests futures to complete
            List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>(responseMap.keySet());
            CompletableFuture<Void> allFutures =
                    CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
            allFutures.join(); // Wait for all futures to complete

            // Collect the responses from the futures
            Map<ResponseItems<String>, List<T>> resultsMap = new HashMap<>(responseMap.size());
            for (Map.Entry<CompletableFuture<ResponseItems<String>>, List<T>> entry : responseMap.entrySet()) {
                resultsMap.put(entry.getKey().join(), entry.getValue());
            }

            return resultsMap;
        }

        /**
         * Process items.
         *
         * Submits a (large) batch of items by splitting it up into multiple, parallel update requests.
         * The response from each request is returned along with the items used as input.
         *
         * @param items the objects to create/insert.
         * @param processFunc function that maps batch to a request
         * @return a {@link Map} with the responses and request inputs.
         */
        private Map<ResponseItems<String>, List<T>> splitAndProcessItems(
                String loggingPrefix,
                List<T> items,
                Function<List<T>, CompletableFuture<ResponseItems<String>>> processFunc) {
            Instant startInstant = Instant.now();
            Map<CompletableFuture<ResponseItems<String>>, List<T>> responseMap = new HashMap<>();

            // Split into batches
            List<List<T>> itemBatches = getBatchingFunction().apply(items);

            // Submit all batches
            for (List<T> batch : itemBatches) {
                responseMap.put(processFunc.apply(batch), batch);
            }
            LOG.debug(loggingPrefix + "Completed building requests for {} items across {} batches at duration {}",
                    items.size(),
                    responseMap.size(),
                    Duration.between(startInstant, Instant.now()).toString());

            // Wait for all requests futures to complete
            List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>(responseMap.keySet());
            CompletableFuture<Void> allFutures =
                    CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
            allFutures.join(); // Wait for all futures to complete

            // Collect the responses from the futures
            Map<ResponseItems<String>, List<T>> resultsMap = new HashMap<>(responseMap.size());
            for (Map.Entry<CompletableFuture<ResponseItems<String>>, List<T>> entry : responseMap.entrySet()) {
                resultsMap.put(entry.getKey().join(), entry.getValue());
            }

            return resultsMap;
        }

        /**
         * Submits a set of items as a create / insert items request to the Cognite API.
         *
         * @param items the objects to create/insert.
         * @return a {@link CompletableFuture} representing the response from the create request.
         * @throws Exception
         */
        private CompletableFuture<ResponseItems<String>> createItems(List<T> items) {
            Preconditions.checkState(null != getCreateItemWriter() && null != getCreateMappingFunction(),
                    "Unable to send item update request. CreateItemWriter and CreateMappingFunction must be specified.");
            return submitRequestAsync(items, getCreateMappingFunction(), getCreateItemWriter());
        }

        /**
         * Submits a set of items as an update items request to the Cognite API.
         *
         * @param items the objects to update.
         * @return a {@link CompletableFuture} representing the response from the update request.
         * @throws Exception
         */
        private CompletableFuture<ResponseItems<String>> updateItems(List<T> items) {
            Preconditions.checkState(null != getUpdateItemWriter() && null != getUpdateMappingFunction(),
                    "Unable to send item update request. UpdateItemWriter and UpdateMappingFunction must be specified.");
            return submitRequestAsync(items, getUpdateMappingFunction(), getUpdateItemWriter());
        }

        /**
         * Submits a set of items as a writer request with mapping function to the Cognite API.
         *
         * @param items  the objects to update.
         * @param itemMapper JSON deserializer function
         * @param writer the {@link com.cognite.client.servicesV1.ConnectorServiceV1.ItemWriter} to use to post the items.
         * @return a {@link CompletableFuture} representing the response from the update request.
         * @throws Exception
         */
        private CompletableFuture<ResponseItems<String>> submitRequestAsync(
                List<T> items,
                @NotNull Function<T, Map<String, Object>> itemMapper,
                @NotNull ConnectorServiceV1.ItemWriter writer) {
            try {
                ImmutableList.Builder<Map<String, Object>> itemsBuilder = ImmutableList.builder();

                for (T item : items) {
                    itemsBuilder.add(itemMapper.apply(item));
                }
                Request writeItemsRequest = Request.create()
                        .withItems(itemsBuilder.build())
                        .withAuthConfig(getAuthConfig());

                return writer.writeItemsAsync(writeItemsRequest);
            } catch (Exception e) {
                throw new IllegalStateException("Unexpected failure when assembling request", e);
            }
        }

        /**
         * Submits a set of items as an update items request to the Cognite API.
         *
         * This version of the update function takes both the existing item and the new item as input. Then these
         * two are compared for differences and the update request sent to CDF.
         *
         * This method is tailored specifically for update of {@link com.cognite.client.dto.SequenceMetadata} where we
         * need to compare existing with new in order to construct the update request for the {@code columns} definitions.
         *
         * @param updateItems the objects to update.
         * @param existingItems the current version of the items in CDF.
         * @return a {@link CompletableFuture} representing the response from the update request.
         * @throws Exception
         */
        private CompletableFuture<ResponseItems<String>> updateItems(List<T> updateItems, List<T> existingItems) {
            Preconditions.checkState(null != getUpdateItemWriter() && null != getUpdateMappingBiFunction(),
                    "Unable to send item update request. UpdateItemWriter and UpdateMappingBiFunction must be specified.");
            Preconditions.checkArgument(updateItems.size() <= existingItems.size(),
                    String.format("Existing items list must be minimum the same size as the update items list. "
                            + "Existing items size: %d. Update items size: %d",
                            updateItems.size(),
                            existingItems.size()));

            Map<String, T> existingItemsMap = mapToId(existingItems);
            Map<String, T> updateItemsMap = mapToId(updateItems);
            ImmutableList.Builder<Map<String, Object>> itemsBuilder = ImmutableList.builder();
            try {
                for (Map.Entry<String, T> updateEntry : updateItemsMap.entrySet()) {
                    if (!existingItemsMap.containsKey(updateEntry.getKey())) {
                        // Should not happen
                        throw new Exception("Error trying to update items. Unable to identify the existing item to "
                                + "compare the update item with. Update item externalId: " + updateEntry.getKey());
                    }
                    itemsBuilder.add(getUpdateMappingBiFunction().apply(updateEntry.getValue(), existingItemsMap.get(updateEntry.getKey())));
                }
                Request writeItemsRequest = Request.create()
                        .withItems(itemsBuilder.build())
                        .withAuthConfig(getAuthConfig());

                return getUpdateItemWriter().writeItemsAsync(writeItemsRequest);
            } catch (Exception e) {
                throw new IllegalStateException("Unexpected failure when assembling request", e);
            }
        }

        /**
         * Checks all items for id / externalId. Returns {@code true} if all items have an id, {@code false}
         * if one (or more) items are missing id / externalId.
         *
         * @param items the items to check for id / externalId
         * @return {@code true} if all items have an id, {@code false} if one (or more) items are missing id / externalId
         */
        private boolean itemsHaveId(List<T> items) {
            for (T item : items) {
                if (!getIdFunction().apply(item).isPresent()) {
                    return false;
                }
            }

            return true;
        }

        /**
         * Maps all items to their externalId (primary) or id (secondary). If the id function does not return any
         * identity, the item will be mapped to the empty string.
         *
         * Via the identity mapping, this function will also perform deduplication of the input items.
         *
         * @param items the items to map to externalId / id.
         * @return the {@link Map} with all items mapped to externalId / id.
         */
        private Map<String, T> mapToId(List<T> items) {
            Map<String, T> resultMap = new HashMap<>((int) (items.size() * 1.35));
            for (T item : items) {
                resultMap.put(getIdFunction().apply(item).orElse(""), item);
            }
            return resultMap;
        }

        /**
         * De-duplicates the input items based on their {@code externalId / id}.
         *
         * @param items The items to be de-duplicated
         * @return a {@link List} with no duplicate items.
         */
        private List<T> deduplicate(List<T> items) {
            return new ArrayList<>(mapToId(items).values());
        }

        @AutoValue.Builder
        abstract static class Builder<T extends Message> {
            abstract Builder<T> setMaxBatchSize(int value);

            abstract Builder<T> setAuthConfig(AuthConfig value);

            abstract Builder<T> setIdFunction(Function<T, Optional<String>> value);

            abstract Builder<T> setCreateMappingFunction(Function<T, Map<String, Object>> value);

            abstract Builder<T> setBatchingFunction(Function<List<T>, List<List<T>>> value);

            abstract Builder<T> setUpdateMappingFunction(Function<T, Map<String, Object>> value);
            abstract Builder<T> setUpdateMappingBiFunction(BiFunction<T, T, Map<String, Object>> value);

            abstract Builder<T> setItemMappingFunction(Function<T, Item> value);

            abstract Builder<T> setRetrieveFunction(Function<List<Item>, List<T>> value);

            abstract Builder<T> setEqualFunction(BiFunction<T, T, Boolean> value);

            abstract Builder<T> setCreateItemWriter(ConnectorServiceV1.ItemWriter value);

            abstract Builder<T> setUpdateItemWriter(ConnectorServiceV1.ItemWriter value);

            abstract Builder<T> setDeleteItemWriter(ConnectorServiceV1.ItemWriter value);

            abstract ImmutableMap.Builder<String, Object> deleteParametersBuilder();

            UpsertItems.Builder<T> addDeleteParameter(String key, Object value) {
                deleteParametersBuilder().put(key, value);
                return this;
            }

            abstract UpsertItems<T> build();
        }
    }

    /**
     * This class handles deletion of items from Cognite Data Fusion.
     *
     * It will also split large delete batches into smaller batches distributed over multiple, parallel
     * requests.
     */
    @AutoValue
    public abstract static class DeleteItems<T extends Message> {
        private static final int DEFAULT_MAX_BATCH_SIZE = 1000;
        private static final int maxDeleteLoopIterations = 4;

        protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

        private static <T extends Message> Builder<T> builder() {
            return new AutoValue_ApiBase_DeleteItems.Builder<T>()
                    .setMaxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                    .setIdFunction(DeleteItems::defaultGetId);
        }

        /**
         * Get id via the common dto methods {@code getExternalId} and {@code getId}.
         *
         * This is the default implementation which uses {@code Message} reflection to find the {@code externalId} or
         * {@code id} fields.
         *
         * @param item The item to interrogate for id
         * @param <T>  The object type of the item. Must be a protobuf object.
         * @return The (external)Id if found. An empty {@link Optional} if no id is found.
         */
        private static <T extends Message> Optional<String> defaultGetId(T item) {
            Optional<String> returnObject = Optional.empty();

            java.util.Map<Descriptors.FieldDescriptor, java.lang.Object> fieldMap = item.getAllFields();
            for (Descriptors.FieldDescriptor field : fieldMap.keySet()) {
                if (field.getName().equalsIgnoreCase("space")
                        && field.getType() == Descriptors.FieldDescriptor.Type.STRING) {
                    returnObject = Optional.of((String) fieldMap.get(field));
                } else if (field.getName().equalsIgnoreCase("external_id")
                        && field.getType() == Descriptors.FieldDescriptor.Type.STRING) {
                    returnObject = Optional.of((String) fieldMap.get(field));
                } else if (field.getName().equalsIgnoreCase("id")
                        && field.getType() == Descriptors.FieldDescriptor.Type.INT64) {
                    returnObject = Optional.of(String.valueOf(fieldMap.get(field)));
                }
            }

            return returnObject;
        }

        /**
         * Get id via for an {@link Item}
         *
         * @param item The item to interrogate for id
         * @return The (external)Id if found. An empty {@link Optional} if no id is found.
         */
        private static Optional<String> defaultItemGetId(Item item) {
            Optional<String> returnObject = Optional.empty();

            if (item.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                returnObject = Optional.of(item.getExternalId());
            } else if (item.getIdTypeCase() == Item.IdTypeCase.ID) {
                returnObject = Optional.of(String.valueOf(item.getId()));
            }
            return returnObject;
        }

        /**
         * The default function for translating an Item to a delete items object.
         *
         * @param item
         * @return
         */
        private static <T extends Message> Map<String, Object> defaultToDeleteItem(T item) {
            java.util.Map<Descriptors.FieldDescriptor, java.lang.Object> fieldMap = item.getAllFields();
            for (Descriptors.FieldDescriptor field : fieldMap.keySet()) {
                if (field.getName().equalsIgnoreCase("space")
                        && field.getType() == Descriptors.FieldDescriptor.Type.STRING) {
                    return Map.of("space", (String) fieldMap.get(field));
                } else if (field.getName().equalsIgnoreCase("external_id")
                        && field.getType() == Descriptors.FieldDescriptor.Type.STRING) {
                    return Map.of("externalId", (String) fieldMap.get(field));
                } else if (field.getName().equalsIgnoreCase("id")
                        && field.getType() == Descriptors.FieldDescriptor.Type.INT64) {
                    return Map.of("id", String.valueOf(fieldMap.get(field)));
                }
            }
            return Collections.emptyMap();
        }

        /**
         * The default function for translating an Item to a delete items object.
         *
         * @param item
         * @return
         */
        private static Map<String, Object> defaultItemToDeleteItem(Item item) {
            if (item.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                return ImmutableMap.of("externalId", item.getExternalId());
            } else if (item.getIdTypeCase() == Item.IdTypeCase.ID) {
                return ImmutableMap.of("id", item.getId());
            } else {
                throw new RuntimeException("Item contains neither externalId nor id.");
            }
        }

        /**
         * Creates a new {@link DeleteItems} that will perform upsert actions.
         *
         * @param deleteWriter The item writer for delete requests.
         * @param authConfig   The authorization info for api requests.
         * @return the {@link DeleteItems} for delete requests.
         */
        public static DeleteItems<Item> ofItem(ConnectorServiceV1.ItemWriter deleteWriter,
                                                AuthConfig authConfig) {
            return DeleteItems.<Item>builder()
                    .setDeleteItemWriter(deleteWriter)
                    .setAuthConfig(authConfig)
                    .setItemMappingFunction(DeleteItems::defaultItemToDeleteItem)
                    .setIdFunction(DeleteItems::defaultItemGetId)
                    .build();
        }

        /**
         * Creates a new {@link DeleteItems} that will perform upsert actions.
         *
         * @param deleteWriter The item writer for delete requests.
         * @param mappingFunction The mapping function for translating input items to object Json.
         * @param authConfig   The authorization info for api requests.
         * @return the {@link DeleteItems} for delete requests.
         */
        public static <T extends Message> DeleteItems<T> of(ConnectorServiceV1.ItemWriter deleteWriter,
                                                            Function<T, Map<String, Object>> mappingFunction,
                                                            AuthConfig authConfig) {
            return DeleteItems.<T>builder()
                    .setDeleteItemWriter(deleteWriter)
                    .setAuthConfig(authConfig)
                    .setItemMappingFunction(mappingFunction)
                    .build();
        }

        abstract Builder<T> toBuilder();

        abstract int getMaxBatchSize();

        abstract AuthConfig getAuthConfig();

        abstract Function<T, Map<String, Object>> getItemMappingFunction();
        abstract Function<T, Optional<String>> getIdFunction();

        abstract ConnectorServiceV1.ItemWriter getDeleteItemWriter();

        abstract ImmutableMap<String, Object> getParameters();

        /**
         * Sets the delete parameters map.
         *
         * This is typically used to add attributes such as {@code ignoreUnknownIds} and / or {@code recursive}.
         *
         * @param parameters The parameter map.
         * @return The {@link DeleteItems} object with the configuration applied.
         */
        public DeleteItems setParameters(Map<String, Object> parameters) {
            return toBuilder().setParameters(parameters).build();
        }

        /**
         * Sets the maximum batch size for delete request.
         *
         * @param batchSize The max bath size for delete requests
         * @return The {@link DeleteItems} object with the configuration applied.
         */
        public DeleteItems withMaxBatchSize(int batchSize) {
            return toBuilder().setMaxBatchSize(batchSize).build();
        }

        /**
         * Adds an attribute to the delete items request.
         *
         * This is typically used to add attributes such as {@code ignoreUnknownIds} and / or {@code recursive}.
         *
         * @param key   The name of the attribute.
         * @param value The value of the attribute
         * @return The {@link DeleteItems} object with the configuration applied.
         */
        public DeleteItems addParameter(String key, Object value) {
            return toBuilder().addParameter(key, value).build();
        }

        /**
         * Sets the delete items mapping function.
         *
         * This function builds the delete item object based on the input {@code elements}. The default function
         * builds delete items based on {@code externalId / id}.
         *
         * @param mappingFunction The mapping function
         * @return The {@link DeleteItems} object with the configuration applied.
         */
        public DeleteItems withItemMappingFunction(Function<T, Map<String, Object>> mappingFunction) {
            return toBuilder().setItemMappingFunction(mappingFunction).build();
        }

        /**
         * Delete a set of items.
         *
         * @param items The items to be deleted.
         * @return The deleted items.
         * @throws Exception
         */
        public List<T> deleteItems(List<T> items) throws Exception {
            Instant startInstant = Instant.now();
            String batchLogPrefix =
                    "deleteItems() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
            Preconditions.checkArgument(itemsHaveId(items),
                    batchLogPrefix + "All items must have an identity.");
            LOG.debug(String.format(batchLogPrefix + "Received %d items to delete",
                    items.size()));

            // Should not happen--but need to guard against empty input
            if (items.isEmpty()) {
                LOG.debug(batchLogPrefix + "Received an empty input list. Will just output an empty list.");
                return Collections.<T>emptyList();
            }

            // Delete and completed lists
            List<T> elementListDelete = deduplicate(items);
            List<T> elementListCompleted = new ArrayList<>(elementListDelete.size());

            if (elementListDelete.size() != items.size()) {
                LOG.debug(batchLogPrefix + "Identified {} duplicate items in the input.",
                        items.size() - elementListDelete.size());
            }

            /*
            The delete loop. If there are items left to delete:
            1. Delete items
            2. If conflict, remove duplicates and missing items.
            */
            ThreadLocalRandom random = ThreadLocalRandom.current();
            String exceptionMessage = "";
            for (int i = 0; i < maxDeleteLoopIterations && elementListDelete.size() > 0;
                 i++, Thread.sleep(Math.min(500L, (10L * (long) Math.exp(i)) + random.nextLong(5)))) {
                LOG.debug(batchLogPrefix + "Start delete loop {} with {} items to delete and "
                                + "{} completed items at duration {}",
                        i,
                        elementListDelete.size(),
                        elementListCompleted.size(),
                        Duration.between(startInstant, Instant.now()).toString());

                /*
                Delete items
                 */
                Map<ResponseItems<String>, List<T>> deleteResponseMap = splitAndDeleteItems(elementListDelete);
                LOG.debug(batchLogPrefix + "Completed delete items requests for {} items across {} batches at duration {}",
                        elementListDelete.size(),
                        deleteResponseMap.size(),
                        Duration.between(startInstant, Instant.now()).toString());
                elementListDelete.clear(); // Must prepare the list for possible new entries.

                for (ResponseItems<String> response : deleteResponseMap.keySet()) {
                    if (response.isSuccessful()) {
                        elementListCompleted.addAll(deleteResponseMap.get(response));
                        LOG.debug(batchLogPrefix + "Delete items request success. Adding {} delete result items to result collection.",
                                deleteResponseMap.get(response).size());
                    } else {
                        exceptionMessage = response.getResponseBodyAsString();
                        LOG.debug(batchLogPrefix + "Delete items request failed: {}", response.getResponseBodyAsString());
                        if (i == maxDeleteLoopIterations - 1) {
                            // Add the error message to std logging
                            LOG.error(batchLogPrefix + "Delete items request failed. {}", response.getResponseBodyAsString());
                        }
                        LOG.debug(batchLogPrefix + "Delete items request failed. "
                                + "Removing duplicates and missing items and retrying the request");
                        List<Item> duplicates = ItemParser.parseItems(response.getDuplicateItems());
                        List<Item> missing = ItemParser.parseItems(response.getMissingItems());
                        LOG.debug(batchLogPrefix + "No of duplicate entries reported by CDF: {}", duplicates.size());
                        LOG.debug(batchLogPrefix + "No of missing items reported by CDF: {}", missing.size());

                        // Remove missing items from the delete request
                        Map<String, T> itemsMap = mapToId(deleteResponseMap.get(response));
                        for (Item value : missing) {
                            if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                                itemsMap.remove(value.getExternalId());
                            } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                                itemsMap.remove(String.valueOf(value.getId()));
                            }
                        }

                        // Remove duplicate items from the delete request
                        for (Item value : duplicates) {
                            if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                                itemsMap.remove(value.getExternalId());
                            } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                                itemsMap.remove(String.valueOf(value.getId()));
                            }
                        }

                        elementListDelete.addAll(itemsMap.values()); // Add remaining items to be re-inserted
                    }
                }
            }

            // Check if all elements completed the upsert requests
            if (elementListDelete.isEmpty()) {
                LOG.info(batchLogPrefix + "Successfully deleted {} items within a duration of {}.",
                        elementListCompleted.size(),
                        Duration.between(startInstant, Instant.now()).toString());
            } else {
                LOG.error(batchLogPrefix + "Failed to delete items. {} items remaining. {} items deleted."
                                + System.lineSeparator() + "{}",
                        elementListDelete.size(),
                        elementListCompleted.size(),
                        exceptionMessage);
                throw new Exception(String.format(batchLogPrefix + "Failed to delete items. %d items remaining. "
                                + " %d items deleted. %n " + exceptionMessage,
                        elementListDelete.size(),
                        elementListCompleted.size()));
            }

            return elementListCompleted;
        }

        /**
         * Delete items.
         *
         * Submits a (large) batch of items by splitting it up into multiple, parallel delete requests.
         * The response from each request is returned along with the items used as input.
         *
         * @param items the objects to delete.
         * @return a {@link Map} with the responses and request inputs.
         * @throws Exception
         */
        private Map<ResponseItems<String>, List<T>> splitAndDeleteItems(List<T> items) throws Exception {
            Map<CompletableFuture<ResponseItems<String>>, List<T>> responseMap = new HashMap<>();
            List<List<T>> itemBatches = Partition.ofSize(items, getMaxBatchSize());

            // Submit all batches
            for (List<T> batch : itemBatches) {
                responseMap.put(deleteBatch(batch), batch);
            }

            // Wait for all requests futures to complete
            List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
            responseMap.keySet().forEach(future -> futureList.add(future));
            CompletableFuture<Void> allFutures =
                    CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
            allFutures.join(); // Wait for all futures to complete

            // Collect the responses from the futures
            Map<ResponseItems<String>, List<T>> resultsMap = new HashMap<>(responseMap.size());
            for (Map.Entry<CompletableFuture<ResponseItems<String>>, List<T>> entry : responseMap.entrySet()) {
                resultsMap.put(entry.getKey().join(), entry.getValue());
            }

            return resultsMap;
        }

        /**
         * Submits a set of items as a delete items request to the Cognite API.
         *
         * @param items the objects to delete.
         * @return a {@link CompletableFuture} representing the response from the create request.
         * @throws Exception
         */
        private CompletableFuture<ResponseItems<String>> deleteBatch(List<T> items) throws Exception {
            ImmutableList.Builder<Map<String, Object>> insertItemsBuilder = ImmutableList.builder();
            for (T item : items) {
                insertItemsBuilder.add(getItemMappingFunction().apply(item));
            }
            Request writeItemsRequest = Request.create()
                    .withItems(insertItemsBuilder.build())
                    .withAuthConfig(getAuthConfig());

            // Add any extra attributes
            for (Map.Entry<String, Object> parameter : getParameters().entrySet()) {
                writeItemsRequest = writeItemsRequest.withRootParameter(parameter.getKey(), parameter.getValue());
            }

            return getDeleteItemWriter().writeItemsAsync(writeItemsRequest);
        }

        /**
         * Checks all items for id / externalId. Returns {@code true} if all items have an id, {@code false}
         * if one (or more) items are missing id / externalId.
         *
         * @param items the items to check for id / externalId
         * @return {@code true} if all items have an id, {@code false} if one (or more) items are missing id / externalId
         */
        private boolean itemsHaveId(List<T> items) {
            for (T item : items) {
                if (!getIdFunction().apply(item).isPresent()) {
                    return false;
                }
            }

            return true;
        }

        /**
         * Maps all items to their externalId (primary) or id (secondary). If the id function does not return any
         * identity, the item will be mapped to the empty string.
         *
         * Via the identity mapping, this function will also perform deduplication of the input items.
         *
         * @param items the items to map to externalId / id.
         * @return the {@link Map} with all items mapped to externalId / id.
         */
        private Map<String, T> mapToId(List<T> items) {
            Map<String, T> resultMap = new HashMap<>((int) (items.size() * 1.35));
            for (T item : items) {
                resultMap.put(getIdFunction().apply(item).orElse(""), item);
            }
            return resultMap;
        }

        /**
         * De-duplicates the input items based on their {@code externalId / id}.
         *
         * @param items The items to be de-duplicated
         * @return a {@link List} with no duplicate items.
         */
        private List<T> deduplicate(List<T> items) {
            return new ArrayList<>(mapToId(items).values());
        }

        @AutoValue.Builder
        abstract static class Builder<T extends Message> {
            abstract Builder<T> setMaxBatchSize(int value);

            abstract Builder<T> setAuthConfig(AuthConfig value);

            abstract Builder<T> setItemMappingFunction(Function<T, Map<String, Object>> value);
            abstract Builder<T> setIdFunction(Function<T, Optional<String>> value);

            abstract Builder<T> setDeleteItemWriter(ConnectorServiceV1.ItemWriter value);

            abstract Builder<T> setParameters(Map<String, Object> value);

            abstract ImmutableMap.Builder<String, Object> parametersBuilder();

            Builder addParameter(String key, Object value) {
                parametersBuilder().put(key, value);
                return this;
            }

            abstract DeleteItems<T> build();
        }
    }

    static abstract class Builder<B extends Builder<B>> {
        abstract B setClient(CogniteClient value);
    }
}
