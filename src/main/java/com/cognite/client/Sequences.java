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
import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.DataSet;
import com.cognite.client.dto.SequenceMetadata;
import com.cognite.client.dto.Item;
import com.cognite.client.queue.UploadQueue;
import com.cognite.client.queue.UpsertTarget;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.SequenceParser;
import com.cognite.client.util.Items;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite sequences api endpoint.
 *
 * It provides for reading an writing {@link SequenceMetadata}
 */
@AutoValue
public abstract class Sequences extends ApiBase implements UpsertTarget<SequenceMetadata, SequenceMetadata> {

    private static Builder builder() {
        return new AutoValue_Sequences.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(Sequences.class);

    /**
     * Construct a new {@link Sequences} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use fir cinfiguration settings.
     * @return The sequences api object.
     */
    public static Sequences of(CogniteClient client) {
        return Sequences.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns {@link SequenceRows} representing the sequences data / rows api.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     client.sequences().rows();
     * }
     * </pre>
     *
     * @see CogniteClient
     * @see CogniteClient#sequences()
     *
     * @return The sequences data / rows api object.
     */
    public SequenceRows rows() {
        return SequenceRows.of(getClient());
    }

    /**
     * Returns all {@link SequenceMetadata} objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<SequenceMetadata> listResults = new ArrayList<>();
     *     client.sequences()
     *                  .list()
     *                  .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Sequences/operation/advancedListSequences">API Reference - Filter sequences</a><br/>
     *
     * @see #list(Request)
     * @see CogniteClient
     * @see CogniteClient#sequences()
     *
     */
    public Iterator<List<SequenceMetadata>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Return all {@link SequenceMetadata} object that matches the filters set in the {@link SequenceMetadata}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into yout own data structure.
     *
     * The sequences are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<SequenceMetadata> listResults = new ArrayList<>();
     *      client.sequences()
     *              .list(Request.create()
     *                             .withFilterParameter("name", "name"))
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Sequences/operation/advancedListSequences">API Reference - Filter sequences</a><br/>
     *
     * @see #list(Request,String...)
     * @see CogniteClient
     * @see CogniteClient#sequences()
     *
     * @param requestParameters The filters to use for retrieving sequences
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<SequenceMetadata>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link SequenceMetadata} objects that matches the filters set in the {@link Request} for
     * the specified partitions. This method is intended for advanced use cases you need direct control over the
     * individual partitions. For example, when using the SDK in a distributed environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<SequenceMetadata> listResults = new ArrayList<>();
     *      client.sequences()
     *              .list(Request.create()
     *                             .withFilterParameter("name", "name"),
     *                                  "1/8","2/8","3/8","4/8","5/8","6/8","7/8","8/8")
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Sequences/operation/advancedListSequences">API Reference - Filter sequences</a><br/>
     *
     * @see #listJson(ResourceType,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#sequences()
     *
     * @param requestParameters The filters to use for retrieving the timeseries.
     * @param partitions The partitions to include
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<SequenceMetadata>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.SEQUENCE_HEADER, requestParameters, partitions), this::parseSequences);
    }

    /**
     * Retrieve sequences by {@code externalId}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<SequenceMetadata> retrievedSequences = client.sequences().retrieve("1","2");
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Sequences/operation/getSequenceById">API Reference - Retrieve sequences</a><br/>
     *
     * @see #retrieve(List)
     * @see CogniteClient
     * @see CogniteClient#sequences()
     *
     * @param externalId The {@code externalIds} to retrieve
     * @return The retrieved sequences.
     * @throws Exception
     */
    public List<SequenceMetadata> retrieve(String... externalId) throws Exception {
        return retrieve(Items.parseItems(externalId));
    }

    /**
     * Retrieve sequences by {@code internal id}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<SequenceMetadata> retrievedSequences = client.sequences().retrieve(1,2);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Sequences/operation/getSequenceById">API Reference - Retrieve sequences</a><br/>
     *
     * @see #retrieve(List)
     * @see CogniteClient
     * @see CogniteClient#sequences()
     *
     * @param id The {@code ids} to retrieve
     * @return The retrieved sequences.
     * @throws Exception
     */
    public List<SequenceMetadata> retrieve(long... id) throws Exception {
        return retrieve(Items.parseItems(id));
    }

    /**
     * Retrieve sequences by {@code externalId / id}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Item> items = List.of(Item.newBuilder().setExternalId("1").build());
     *      List<SequenceMetadata> retrievedSequences = client.sequences().retrieve(items);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Sequences/operation/getSequenceById">API Reference - Retrieve sequences</a><br/>
     *
     * @see #retrieveJson(ResourceType,Collection)
     * @see CogniteClient
     * @see CogniteClient#sequences()
     *
     * @param items The item(s) {@code externalId / id} to retrieve
     * @return The retrieved sequences
     * @throws Exception
     */
    public List<SequenceMetadata> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.SEQUENCE_HEADER, items).stream()
                .map(this::parseSequences)
                .collect(Collectors.toList());
    }

    /**
     * Performs an item aggregation request to Cognite Data Fusion.
     *
     * The default aggregation is a total item count based on the (optional) filters in the request.
     * Multiple aggregation types are supported, Please refer to the Cognite API specification for more information
     * on the possible settings.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Aggregate aggregateResult = client.sequences()
     *                  .aggregate(Request.create()
     *                  .withFilterParameter("name", "name"));
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Sequences/operation/aggregateSequences">API Reference - Aggregate sequences</a><br/>
     *
     * @see #aggregate(ResourceType,Request)
     * @see CogniteClient
     * @see CogniteClient#sequences()
     *
     * @param requestParameters The filtering and aggregates specification.
     * @return The aggregation results.
     * @throws Exception
     * @see <a href="https://docs.cognite.com/api/v1/">Cognite API v1 specification</a>
     */
    public Aggregate aggregate(Request requestParameters) throws Exception {
        return aggregate(ResourceType.SEQUENCE_HEADER, requestParameters);
    }

    /**
     * Creates or update a set of {@link SequenceMetadata} objects.
     *
     * If it is a new {@link SequenceMetadata} object already exists in Cognite Data Fusion, it will be updated. The
     * update behaviour is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<SequenceMetadata> sequences = // List of SequenceMetadata;
     *      client.sequences().upsert(sequences);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Sequences/operation/createSequence">API Reference - Create sequences</a><br/>
     * <a href="https://docs.cognite.com/api/v1/#tag/Sequences/operation/updateSequences">API Reference - Update sequences</a>
     *
     * @see UpsertItems#upsertViaGetCreateAndUpdateDiff(List)
     * @see CogniteClient
     * @see CogniteClient#sequences()
     *
     * @param sequences The sequences to upsert
     * @return The upserted sequences
     * @throws Exception
     */
    public List<SequenceMetadata> upsert(List<SequenceMetadata> sequences) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeSequencesHeaders();
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateSequencesHeaders();

        UpsertItems<SequenceMetadata> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withUpdateItemWriter(updateItemWriter)
                .withItemMappingFunction(this::toItem)
                .withRetrieveFunction(this::retrieveWrapper)
                .withUpdateMappingBiFunction(this::toRequestUpdateItem)
                .withIdFunction(this::getSequenceId)
                .withEqualFunction((SequenceMetadata a, SequenceMetadata b) ->
                        a.getId() == b.getId() || (a.hasExternalId() && b.hasExternalId() && a.getExternalId().equals(b.getExternalId())))
                .withBatchingFunction(this::splitSequenceMetadataIntoBatches);

        if (getClient().getClientConfig().getUpsertMode() == UpsertMode.REPLACE) {
            upsertItems = upsertItems.withUpdateMappingBiFunction(this::toRequestReplaceItem);
        }

        return upsertItems.upsertViaGetCreateAndUpdateDiff(sequences).stream()
                .map(this::parseSequences)
                .collect(Collectors.toList());
    }

    /**
     * Returns an upload queue.
     *
     * The upload queue helps improve performance by batching items together before uploading them to Cognite Data Fusion.
     * @return The upload queue.
     */
    public UploadQueue<SequenceMetadata, SequenceMetadata> uploadQueue() {
        return UploadQueue.of(this);
    }

    /**
     * Deletes a set of Sequences.
     *
     * The sequences to delete are identified via their {@code externalId / id} by submitting a list of {@link Item}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<Item> sequences = List.of(Item.newBuilder().setExternalId("1").build());
     *     List<Item> deletedItemsResults = client.sequences().delete(sequences);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Sequences/operation/deleteSequences">API Reference - Delete sequences</a>
     *
     * @see DeleteItems#deleteItems(List)
     * @see CogniteClient
     * @see CogniteClient#sequences()
     *
     * @param sequences A list of {@link Item} representing the sequences (externalId / id) to be deleted
     * @return The deleted sequences via {@link Item}
     * @throws Exception
     */
    public List<Item> delete(List<Item> sequences) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteSequencesHeaders();

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildAuthConfig());

        return deleteItems.deleteItems(sequences);
    }

    /*
    Custom batching function for upserting sequence headers/metadata. There are two limitations for the sequence header
    batch size: 1) max no items = 1k and 2) max no cells/columns = 10k
     */
    private List<List<SequenceMetadata>> splitSequenceMetadataIntoBatches(List<SequenceMetadata> items) {
        int MAX_CELLS_PER_BATCH = 10000;
        int MAX_ITEMS_PER_BATCH = 1000;
        List<List<SequenceMetadata>> batchList = new ArrayList<>();

        int batchCellsCounter = 0;
        List<SequenceMetadata> currentBatch = new ArrayList<>();
        for (SequenceMetadata sequenceMetadata : items) {
            if ((currentBatch.size() + 1 > MAX_ITEMS_PER_BATCH)
                    || (batchCellsCounter + sequenceMetadata.getColumnsCount()) > MAX_CELLS_PER_BATCH) {
                // We cannot add more items to the current batch. Start a new batch
                batchList.add(currentBatch);
                currentBatch = new ArrayList<>();
                batchCellsCounter = 0;
            }
            // Add item to batch and bump the cell counter.
            currentBatch.add(sequenceMetadata);
            batchCellsCounter += sequenceMetadata.getColumnsCount();
        }
        if (currentBatch.size() > 0) {
            // Add all remaining items
            batchList.add(currentBatch);
        }
        return batchList;
    }

    /*
    Returns an Item reflecting the input dataset. This will extract the dataset externalId
    and populate the Item with it.
     */
    private Item toItem(SequenceMetadata sequenceMetadata) {
        return Item.newBuilder().setExternalId(sequenceMetadata.getExternalId()).build();
    }

    /*
    Wrapping the retrieve function because we need to handle the exception--an ugly workaround since lambdas don't deal very well
    with exceptions.
     */
    private List<SequenceMetadata> retrieveWrapper(List<Item> items) {
        try {
            return retrieve(items);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exception.
     */
    private SequenceMetadata parseSequences(String json) {
        try {
            return SequenceParser.parseSequenceMetadata(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(SequenceMetadata item) {
        try {
            return SequenceParser.toRequestInsertItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestUpdateItem(SequenceMetadata existingItem, SequenceMetadata updatedItem) {
        try {
            return SequenceParser.toRequestUpdateItem(existingItem, updatedItem);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestReplaceItem(SequenceMetadata existingItem, SequenceMetadata updatedItem) {
        try {
            return SequenceParser.toRequestReplaceItem(existingItem, updatedItem);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of an event. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getSequenceId(SequenceMetadata item) {
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
        abstract  Sequences build();
    }
}
