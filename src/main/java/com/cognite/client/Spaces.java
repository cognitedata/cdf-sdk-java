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
import com.cognite.client.dto.DataSet;
import com.cognite.client.dto.datamodel.Space;
import com.cognite.client.dto.datamodel.SpaceReference;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.datamodel.SpacesParser;
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
     * Return all {@link Space} objects.
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
        return AdapterIterator.of(listJson(ResourceType.SPACE, requestParameters), this::parseSpace);
    }

    /**
     * Retrieve spaces by {@code spaceIds}.
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
     * @param spaceIds The {@code spaceIds} to retrieve
     * @return The retrieved datasets.
     * @throws Exception
     */
    public List<Space> retrieve(String... spaceIds) throws Exception {
        // Parse ids to SpaceReference objects
        List<SpaceReference> spaceReferences = new ArrayList<>();
        for (String spaceId : spaceIds) {
            spaceReferences.add(SpaceReference.newBuilder().setSpace(spaceId).build());
        }

        return retrieve(spaceReferences);
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
     * @param spaces The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved datasets.
     * @throws Exception
     */
    public List<Space> retrieve(List<SpaceReference> spaces) throws Exception {
        // Parse SpaceReference to Map<String, Object> Json-like item objects
        List<Map<String, Object>> spaceItemList = spaces.stream()
                .map(spaceReference -> Map.of("space", (Object) spaceReference.getSpace()))
                .toList();

        return retrieveJson(ResourceType.SPACE, spaceItemList, Map.of()).stream()
                .map(this::parseSpace)
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
     * @param spaces The spaces to upsert
     * @return The upserted datasets
     * @throws Exception
     */
    public List<Space> upsert(List<Space> spaces) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.upsertSpaces();

        UpsertItems<Space> upsertItems = UpsertItems.of(createItemWriter, this::toRequestUpsertItem, getClient().buildAuthConfig())
                .withIdFunction(this::getSpaceId);  // Must have an id-function when using create as the upsert function

        return upsertItems
                .create(spaces).stream()
                .map(this::parseSpace)
                .collect(Collectors.toList());
    }

    /*
    Returns the id of a space.
     */
    private Optional<String> getSpaceId(Space item) {
        return Optional.of(item.getSpace());
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't deal very well
    with exceptions.
     */
    private Space parseSpace(String json) {
        try {
            return SpacesParser.parseSpace(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't deal very well
    with exceptions.
     */
    private Map<String, Object> toRequestUpsertItem(Space item) {
        try {
            return SpacesParser.toRequestUpsertItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract Spaces build();
    }
}
