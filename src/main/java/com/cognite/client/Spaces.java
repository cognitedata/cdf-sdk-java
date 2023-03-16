/*
 * Copyright (c) 2023 Cognite AS
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
import com.cognite.client.dto.Item;
import com.cognite.client.dto.datamodel.DataModel;
import com.cognite.client.dto.datamodel.Space;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.DataSetParser;
import com.cognite.client.util.Items;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite data model spaces api endpoint
 *
 * It provides methods for reading and writing {@link Space}
 */
@AutoValue
public abstract class Spaces extends ApiBase {

    protected static final Logger LOG = LoggerFactory.getLogger(Spaces.class);

    private static Builder builder() {
        return new AutoValue_Spaces.Builder();
    }

    /**
     * Construct a new {@link Spaces} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return The datasets api object.
     */
    public static Spaces of(CogniteClient client) {
        return Spaces.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link Space} objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<DataSet> listDataSetsResults = new ArrayList<>();
     *     client.datasets()
     *           .list()
     *           .forEachRemaining(batch -> listDataSetsResults.addAll(batch));
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Data-sets/operation/listDataSets">API Reference - Filter data sets</a>
     *
     * @see #list(Request)
     * @see CogniteClient
     * @see CogniteClient#datasets()
     */
    public Iterator<List<Space>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Return all {@link Space} object that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer entire results set, then you have to
     * stream these results into your own data structure.
     *
     * The datasets are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<DataSet> listDataSetsResults = new ArrayList<>();
     *     client.datasets()
     *           .list(Request.create()
     *                            .withFilterParameter("writeProtected", true))
     *           .forEachRemaining(batch -> listDataSetsResults.addAll(batch));
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Data-sets/operation/listDataSets">API Reference - Filter data sets</a>
     *
     * @see #list(Request,String...)
     * @see CogniteClient
     * @see CogniteClient#datasets()
     *
     * @param requestParameters The filters to use for retrieving datasets.
     * @return An {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Space>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Return all {@link Space} objects that matches the filters set in the {@link Request} for the specific
     * partitions. This method is intended for advanced use cases where you need direct control over the individual
     * partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is now buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you have
     * to stream these results into your own data strcture.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<DataSet> listDataSetsResults = new ArrayList<>();
     *     client.datasets()
     *           .list(Request.create()
     *                            .withFilterParameter("writeProtected", true),
     *                                 "1/8","2/8","3/8","4/8","5/8","6/8","7/8","8/8")
     *           .forEachRemaining(batch -> listDataSetsResults.addAll(batch));
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Data-sets/operation/listDataSets">API Reference - Filter data sets</a>
     *
     * @see #listJson(ResourceType,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#datasets()
     *
     * @param requestParameters The filters to use for retrieving the datasets
     * @param partitions        The partitions to include
     * @return An {@link Iterator} to page through the results set
     * @throws Exception
     */
    public Iterator<List<Space>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.DATA_SET, requestParameters, partitions), this::parseDatasets);
    }

    /**
     * Retrieve datasets by {@code externalId}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<DataSet> retrievedDataSets = client.datasets().retrieve("1","2");
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Data-sets/operation/getDataSets">API Reference - Retrieve data sets</a>
     *
     * @see #retrieve(List)
     * @see CogniteClient
     * @see CogniteClient#datasets()
     *
     * @param externalId The {@code externalIds} to retrieve
     * @return The retrieved datasets.
     * @throws Exception
     */
    public List<Space> retrieve(String... externalId) throws Exception {
        return retrieve(Items.parseItems(externalId));
    }

    /**
     * Retrieve datasets by {@code internal id}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<DataSet> retrievedDataSets = client.datasets().retrieve(1,2);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Data-sets/operation/getDataSets">API Reference - Retrieve data sets</a>
     *
     * @see #retrieve(List)
     * @see CogniteClient
     * @see CogniteClient#datasets()
     *
     * @param id The {@code ids} to retrieve
     * @return The retrieved datasets.
     * @throws Exception
     */
    public List<Space> retrieve(long... id) throws Exception {
        return retrieve(Items.parseItems(id));
    }

    /**
     * Retrieves datasets by {@code externalId / id}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Item> items = List.of(Item.newBuilder().setExternalId("1").build());
     *      List<DataSet> retrievedDataSets = client.datasets().retrieve(items);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Data-sets/operation/getDataSets">API Reference - Retrieve data sets</a>
     *
     * @see #retrieveJson(ResourceType, Collection)
     * @see CogniteClient
     * @see CogniteClient#datasets()
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved datasets.
     * @throws Exception
     */
    public List<Space> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.DATA_SET, items).stream()
                .map(this::parseDatasets)
                .collect(Collectors.toList());
    }

    /**
     * Creates or update a set of {@link Space} objects.
     *
     * If it is a new {@link Space} object (based on the {@code id / externalId}, thenit will be created.
     *
     * If an {@link Iterator} object already exists in Cognite Data Fusion, it will be updated. The update behaviour is
     * specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<DataSet> upsertDataSetsList = // List of DataSet;
     *      client.datasets().upsert(upsertDataSetsList);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Data-sets/operation/createDataSets">API Reference - Create data sets</a><br/>
     * <a href="https://docs.cognite.com/api/v1/#tag/Data-sets/operation/updateDataSets">API Reference - Update the attributes of data sets.</a>
     *
     * @see CogniteClient
     * @see CogniteClient#datasets()
     *
     * @param datasets The datasets to upsert
     * @return The upserted datasets
     * @throws Exception
     */
    public List<Space> upsert(List<Space> datasets) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeDataSets();
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateDataSets();

        UpsertItems<Space> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withUpdateItemWriter(updateItemWriter)
                .withUpdateMappingFunction(this::toRequestUpdateItem)
                .withIdFunction(this::getDatasetId);

        if (getClient().getClientConfig().getUpsertMode() == UpsertMode.REPLACE) {
            upsertItems = upsertItems.withUpdateMappingFunction(this::toRequestReplaceItem);
        }

        return upsertItems
                .withRetrieveFunction(this::retrieveWrapper)
                .withItemMappingFunction(this::toItem)  // used by upsertViaGetCreateAndUpdate
                .withEqualFunction((DataSet a, DataSet b) ->
                        a.getId() == b.getId() || (a.hasExternalId() && b.hasExternalId() && a.getExternalId().equals(b.getExternalId())))
                .upsertViaGetCreateAndUpdate(datasets).stream()
                .map(this::parseDatasets)
                .collect(Collectors.toList());
    }

    /*
    Returns an Item reflecting the input dataset. This will extract the dataset externalId
    and populate the Item with it.
     */
    private Item toItem(DataSet dataSet) {
        return Item.newBuilder().setExternalId(dataSet.getExternalId()).build();
    }

    /*
    Wrapping the retrieve function because we need to handle the exception--an ugly workaround since lambdas don't deal very well
    with exceptions.
     */
    private List<Space> retrieveWrapper(List<Item> items) {
        try {
            return retrieve(items);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't deal very well
    with exceptions.
     */
    private DataSet parseDatasets(String json) {
        try {
            return DataSetParser.parseDataSet(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't deal very well
    with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(DataSet item) {
        try {
            return DataSetParser.toRequestInsertItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't deal very well
    with exceptions.
     */
    private Map<String, Object> toRequestUpdateItem(DataSet item) {
        try {
            return DataSetParser.toRequestUpdateItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't deal very well
    with exceptions.
     */
    private Map<String, Object> toRequestReplaceItem(DataSet item) {
        try {
            return DataSetParser.toRequestReplaceItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of a dataset. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getDatasetId(DataSet item) {
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
        abstract Spaces build();
    }
}
