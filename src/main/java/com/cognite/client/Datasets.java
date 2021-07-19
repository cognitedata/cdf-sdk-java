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
import com.cognite.client.dto.*;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.DataSetParser;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite datasets api endpoint
 *
 * It provides methods for reading and writing {@link DataSet}
 */
@AutoValue
public abstract class Datasets extends ApiBase {

    private static Builder builder() {
        return new AutoValue_Datasets.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(Datasets.class);

    /**
     * Construct a new {@link Datasets} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return The datasets api object.
     */
    public static Datasets of(CogniteClient client) {
        return Datasets.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link DataSet} objects.
     *
     * @see #list(Request)
     */
    public Iterator<List<DataSet>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Return all {@link DataSet} object that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer entire results set, then you have to
     * stream these results into your own data structure.
     *
     * The datasets are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters The filters to use for retrieving datasets.
     * @return An {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<DataSet>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Return all {@link DataSet} objects that matches the filters set in the {@link Request} for the specific
     * partitions. This method is intended for advanced use cases where you need direct control over the individual
     * partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is now buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you have
     * to stream these results into your own data strcture.
     *
     * @param requestParameters The filters to use for retrieving the datasets
     * @param partitions The partitions to include
     * @return An {@link Iterator} to page through the results set
     * @throws Exception
     */
    public Iterator<List<DataSet>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.DATA_SET, requestParameters, partitions), this::parseDatasets);
    }

    /**
     * Retrieves datasets by id.
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved datasets.
     * @throws Exception
     */
    public List<DataSet> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.DATA_SET, items).stream()
                .map(this::parseDatasets)
                .collect(Collectors.toList());
    }

    /**
     * Performs an item aggregation to Cognite Data Fusion.
     *
     * The default aggregation is a total item count based on the (optional) filters in the request. Multiple
     * aggregation types are supported. Please refer to the Cognite API specification for more information on the
     * possible settings.
     *
     * @param requestParameters The filtering and aggregates specification.
     * @return The aggregation results.
     * @throws Exception
     * @see <a href="https://docs.cognite.com/api/v1/">Cognite API v1 specification</a>
     */
    public Aggregate aggregate(Request requestParameters) throws Exception {
        return aggregate(ResourceType.DATA_SET, requestParameters);
    }

    /**
     * Creates or update a set of {@link DataSet} objects.
     *
     * If it si a new {@link DataSet} object (based on the {@code id / externalId}, thenit will be created.
     *
     * If an {@link Iterator} object already exists in Cognite Data Fusion, it will be updated. The update behaviour is
     * specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * @param datasets The datasets to upsert
     * @return The upserted datasets
     * @throws Exception
     */
    public List<DataSet> upsert(List<DataSet> datasets) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeDataSets();
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateDataSets();

        UpsertItems<DataSet> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withUpdateItemWriter(updateItemWriter)
                .withUpdateMappingFunction(this::toRequestUpdateItem);

        if (getClient().getClientConfig().getUpsertMode() == UpsertMode.REPLACE) {
            upsertItems = upsertItems.withUpdateMappingFunction(this::toRequestReplaceItem);
        }

        return upsertItems.upsertViaCreateAndUpdate(datasets).stream()
                .map(this::parseDatasets)
                .collect(Collectors.toList());
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
        abstract Datasets build();
    }
}
