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
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.ExtractionPipeline;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.ExtractionPipelineParser;
import com.cognite.client.util.Items;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite extraction pipelines api endpoint.
 *
 * It provides methods for reading and writing {@link Event}.
 */
@AutoValue
public abstract class ExtractionPipelines extends ApiBase {

    private static Builder builder() {
        return new AutoValue_ExtractionPipelines.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(ExtractionPipelines.class);

    /**
     * Constructs a new {@link ExtractionPipelines} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static ExtractionPipelines of(CogniteClient client) {
        return ExtractionPipelines.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns {@link ExtractionPipelineRuns} representing the extraction pipeline runs api endpoint.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     client.extractionPipelines().runs();
     * }
     * </pre>
     *
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     *
     * @return the extraction pipeline runs api object
     */
    public ExtractionPipelineRuns runs() {
        return ExtractionPipelineRuns.of(getClient());
    }

    /**
     * Returns all {@link ExtractionPipeline} objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<ExtractionPipeline> listResults = new ArrayList<>();
     *     client.extractionPipelines()
     *                     .list()
     *                     .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Extraction-Pipelines/operation/filterExtPipes">API Reference - Filter extraction pipelines</a>
     *
     * @see #list(Request)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     */
    public Iterator<List<ExtractionPipeline>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Returns all {@link ExtractionPipeline} objects that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<ExtractionPipeline> listResults = new ArrayList<>();
     *      client.extractionPipelines()
     *              .list(Request.create()
     *                             .withFilterParameter("source", "source"))
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Extraction-Pipelines/operation/filterExtPipes">API Reference - Filter extraction pipelines</a>
     *
     * @see #list(Request,String...)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ExtractionPipeline>> list(Request requestParameters) throws Exception {
        /*
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
         */

        // The extraction pipeline API endpoint does not support partitions (yet). Therefore no partitions are used here
        // in the list implementation.

        return list(requestParameters, new String[0]);
    }

    /**
     * Returns all {@link ExtractionPipeline} objects that matches the filters set in the {@link Request} for the
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
     *      List<ExtractionPipeline> listResults = new ArrayList<>();
     *      client.extractionPipelines()
     *              .list(Request.create()
     *                             .withFilterParameter("source", "source"),
     *                                  "1/8","2/8","3/8","4/8","5/8","6/8","7/8","8/8")
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Extraction-Pipelines/operation/filterExtPipes">API Reference - Filter extraction pipelines</a>
     *
     * @see #listJson(ResourceType,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ExtractionPipeline>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.EXTRACTION_PIPELINE, requestParameters, partitions), this::parseExtractionPipeline);
    }

    /**
     * Retrieve extraction pipelines by {@code externalId}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<ExtractionPipeline> retrievedExtractionPipeline =
     *                          client.extractionPipelines().retrieve("1","2");
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Extraction-Pipelines/operation/showExtPipe">API Reference - Retrieve an extraction pipeline by its ID.</a>
     *
     * @see #retrieve(List)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     *
     * @param externalId The {@code externalIds} to retrieve
     * @return The retrieved extraction pipelines.
     * @throws Exception
     */
    public List<ExtractionPipeline> retrieve(String... externalId) throws Exception {
        return retrieve(Items.parseItems(externalId));
    }

    /**
     * Retrieve extraction pipelines by {@code internal id}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<ExtractionPipeline> retrievedExtractionPipeline =
     *                          client.extractionPipelines().retrieve(1,2);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Extraction-Pipelines/operation/showExtPipe">API Reference - Retrieve an extraction pipeline by its ID.</a>
     *
     * @see #retrieve(List)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     *
     * @param id The {@code ids} to retrieve
     * @return The retrieved extraction pipelines.
     * @throws Exception
     */
    public List<ExtractionPipeline> retrieve(long... id) throws Exception {
        return retrieve(Items.parseItems(id));
    }

    /**
     * Retrieve extraction pipelines by id.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Item> items = List.of(Item.newBuilder().setExternalId("1").build());
     *      List<ExtractionPipeline> retrievedExtractionPipeline =
     *                          client.extractionPipelines().retrieve(items);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Extraction-Pipelines/operation/showExtPipe">API Reference - Retrieve an extraction pipeline by its ID.</a>
     *
     * @see #retrieveJson(ResourceType, Collection)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved extraction pipelines.
     * @throws Exception
     */
    public List<ExtractionPipeline> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.EXTRACTION_PIPELINE, items).stream()
                .map(this::parseExtractionPipeline)
                .collect(Collectors.toList());
    }

    /**
     * Creates or updates a set of {@link ExtractionPipeline} objects.
     *
     * If it is a new {@link ExtractionPipeline} object (based on {@code id / externalId}, then it will be created.
     *
     * If an {@link ExtractionPipeline} object already exists in Cognite Data Fusion, it will be updated. The update behavior
     * is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<ExtractionPipeline> pipelines = // List of Events;
     *      client.extractionPipelines().upsert(pipelines);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Extraction-Pipelines/operation/createExtPipes">API Reference - Create extraction pipelines</a><br/>
     * <a href="https://docs.cognite.com/api/v1/#tag/Extraction-Pipelines/operation/updateExtPipes">API Reference - Update extraction pipelines</a>
     *
     * @see UpsertItems#upsertViaCreateAndUpdate(List)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     *
     * @param pipelines The extraction pipelines to upsert.
     * @return The upserted extraction pipelines.
     * @throws Exception
     */
    public List<ExtractionPipeline> upsert(List<ExtractionPipeline> pipelines) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeExtractionPipelines();
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateExtractionPipelines();

        UpsertItems<ExtractionPipeline> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withUpdateItemWriter(updateItemWriter)
                .withUpdateMappingFunction(this::toRequestUpdateItem)
                .withIdFunction(this::getExtractionPipelineId);

        if (getClient().getClientConfig().getUpsertMode() == UpsertMode.REPLACE) {
            upsertItems = upsertItems.withUpdateMappingFunction(this::toRequestReplaceItem);
        }

        return upsertItems.upsertViaCreateAndUpdate(pipelines).stream()
                .map(this::parseExtractionPipeline)
                .collect(Collectors.toList());
    }

    /**
     * Deletes a set of extraction pipelines.
     *
     * The extraction pipelines to delete are identified via their {@code externalId / id} by submitting a list of
     * {@link Item}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<Item> pipelines = List.of(Item.newBuilder().setExternalId("1").build());
     *     List<Item> deletedItemsResults = client.extractionPipelines().delete(pipelines);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Extraction-Pipelines/operation/deleteExtPipes">API Reference - Delete extraction pipelines</a>
     *
     * @see DeleteItems#deleteItems(List)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     *
     * @param pipelines a list of {@link Item} representing the extraction pipelines (externalId / id) to be deleted
     * @return The deleted extraction pipelines via {@link Item}
     * @throws Exception
     */
    public List<Item> delete(List<Item> pipelines) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteExtractionPipelines();

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildAuthConfig())
                //.addParameter("ignoreUnknownIds", true)
                ;

        return deleteItems.deleteItems(pipelines);
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private ExtractionPipeline parseExtractionPipeline(String json) {
        try {
            return ExtractionPipelineParser.parseExtractionPipeline(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(ExtractionPipeline item) {
        try {
            return ExtractionPipelineParser.toRequestInsertItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestUpdateItem(ExtractionPipeline item) {
        try {
            return ExtractionPipelineParser.toRequestUpdateItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestReplaceItem(ExtractionPipeline item) {
        try {
            return ExtractionPipelineParser.toRequestReplaceItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of an extraction pipeline. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getExtractionPipelineId(ExtractionPipeline item) {
        if (item.hasExternalId()) {
            return Optional.of(item.getExternalId());
        } else if (item.hasId()) {
            return Optional.of(String.valueOf(item.getId()));
        } else {
            return Optional.<String>empty();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract ExtractionPipelines build();
    }
}
