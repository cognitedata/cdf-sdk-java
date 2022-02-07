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
import com.cognite.client.dto.TimeseriesMetadata;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.TimeseriesParser;
import com.cognite.client.util.Items;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite timeseries api endpoint.
 *
 * It provides methods for reading and writing {@link TimeseriesMetadata}.
 */
@AutoValue
public abstract class Timeseries extends ApiBase {

    private static Builder builder() {
        return new AutoValue_Timeseries.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(Timeseries.class);

    /**
     * Construct a new {@link Timeseries} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static Timeseries of(CogniteClient client) {
        return Timeseries.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns {@link DataPoints} representing the time series data points api.
     *
     * @return The time series data points api object.
     */
    public DataPoints dataPoints() {
        return DataPoints.of(getClient());
    }

    /**
     * Returns all {@link TimeseriesMetadata} objects.
     *
     * @see #list(Request)
     */
    public Iterator<List<TimeseriesMetadata>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Returns all {@link TimeseriesMetadata} object that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The timeseries are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters the filters to use for retrieving timeseries.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<TimeseriesMetadata>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link TimeseriesMetadata} objects that matches the filters set in the {@link Request} for
     * the specified partitions. This method is intended for advanced use cases where you need direct control over the
     * individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters the filters to use for retrieving the timeseries.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<TimeseriesMetadata>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.TIMESERIES_HEADER, requestParameters, partitions), this::parseTimeseries);
    }

    /**
     * Retrieve timeseries by {@code externalId}.
     *
     * @param externalId The {@code externalIds} to retrieve
     * @return The retrieved timeseries.
     * @throws Exception
     */
    public List<TimeseriesMetadata> retrieve(String... externalId) throws Exception {
        return retrieve(Items.parseItems(externalId));
    }

    /**
     * Retrieve timeseries by {@code internal id}.
     *
     * @param id The {@code ids} to retrieve
     * @return The retrieved timeseries.
     * @throws Exception
     */
    public List<TimeseriesMetadata> retrieve(long... id) throws Exception {
        return retrieve(Items.parseItems(id));
    }

    /**
     * Retrieves timeseries by {@code externalId / id}.
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved timeseries.
     * @throws Exception
     */
    public List<TimeseriesMetadata> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.TIMESERIES_HEADER, items).stream()
                .map(this::parseTimeseries)
                .collect(Collectors.toList());
    }

    /**
     * Performs an item aggregation request to Cognite Data Fusion.
     *
     * The default aggregation is a total item count based on the (optional) filters in the request.
     * Multiple aggregation types are supported. Please refer to the Cognite API specification for more information
     * on the possible settings.
     *
     * @param requestParameters The filtering and aggregates specification.
     * @return The aggregation results.
     * @throws Exception
     * @see <a href="https://docs.cognite.com/api/v1/">Cognite API v1 specification</a>
     */
    public Aggregate aggregate(Request requestParameters) throws Exception {
        return aggregate(ResourceType.TIMESERIES_HEADER, requestParameters);
    }

    /**
     * Creates or update a set of {@link TimeseriesMetadata} objects.
     *
     * If it is a new {@link TimeseriesMetadata} object (based on the {@code id / externalId}, then it will be created.
     *
     * If an {@link TimeseriesMetadata} object already exists in Cognite Data Fusion, it will be updated. The update
     * behaviour is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * @param timeseries The timeseries to upsert
     * @return The upserted timeseries
     * @throws Exception
     */
    public List<TimeseriesMetadata> upsert(List<TimeseriesMetadata> timeseries) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeTsHeaders();
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateTsHeaders();

        UpsertItems<TimeseriesMetadata> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withUpdateItemWriter(updateItemWriter)
                .withUpdateMappingFunction(this::toRequestUpdateItem)
                .withIdFunction(this::getTimeseriesId);

        if (getClient().getClientConfig().getUpsertMode() == UpsertMode.REPLACE) {
            upsertItems = upsertItems.withUpdateMappingFunction(this::toRequestReplaceItem);
        }

        return upsertItems.upsertViaCreateAndUpdate(timeseries).stream()
                .map(this::parseTimeseries)
                .collect(Collectors.toList());
    }

    public List<Item> delete(List<Item> timeseries) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteTsHeaders();

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildAuthConfig())
                .addParameter("ignoreUnknownIds", true);

        return deleteItems.deleteItems(timeseries);
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private TimeseriesMetadata parseTimeseries(String json) {
        try {
            return TimeseriesParser.parseTimeseriesMetadata(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(TimeseriesMetadata item) {
        try {
            return TimeseriesParser.toRequestInsertItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestUpdateItem(TimeseriesMetadata item) {
        try {
            return TimeseriesParser.toRequestUpdateItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestReplaceItem(TimeseriesMetadata item) {
        try {
            return TimeseriesParser.toRequestReplaceItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of a time series. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getTimeseriesId(TimeseriesMetadata item) {
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
        abstract Timeseries build();
    }
}
