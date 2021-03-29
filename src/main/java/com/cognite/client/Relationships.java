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
import com.cognite.client.dto.Item;
import com.cognite.client.dto.Relationship;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.RelationshipParser;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite relationships api endpoint.
 *
 * It provides methods for reading and writing {@link Relationship}.
 */
@AutoValue
public abstract class Relationships extends ApiBase {

    private static Builder builder() {
        return new AutoValue_Relationships.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(Relationships.class);

    /**
     * Constructs a new {@link Relationships} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static Relationships of(CogniteClient client) {
        return Relationships.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link Relationship} objects that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The assets are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Relationship>> list(Request requestParameters) throws Exception {
        /*
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
         */

        // The relationships API endpoint does not support partitions (yet). Therefore no partitions are used here
        // in the list implementation. Convert to using partitions when the relationship endpoint is updated.

        return list(requestParameters, new String[0]);
    }

    /**
     * Returns all {@link Relationship} objects that matches the filters set in the {@link Request} for the
     * specified partitions. This is method is intended for advanced use cases where you need direct control over
     * the individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Relationship>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.RELATIONSHIP, requestParameters, partitions), this::parseRelationship);
    }

    /**
     * Retrieve Relationships by id.
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved relationships.
     * @throws Exception
     */
    public List<Relationship> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.RELATIONSHIP, items).stream()
                .map(this::parseRelationship)
                .collect(Collectors.toList());
    }


    /**
     * Creates or updates a set of {@link Relationship} objects.
     *
     * If it is a new {@link Relationship} object (based on {@code id / externalId}, then it will be created.
     *
     * If an {@link Relationship} object already exists in Cognite Data Fusion, it will be updated. The update behavior
     * is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * @param relationships The relationships to upsert.
     * @return The upserted relationships.
     * @throws Exception
     */
    public List<Relationship> upsert(List<Relationship> relationships) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeRelationships();
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateRelationships();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteRelationships();

        UpsertItems<Relationship> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withDeleteItemWriter(deleteItemWriter)
                .withUpdateItemWriter(updateItemWriter)
                .withUpdateMappingFunction(this::toRequestUpdateItem)
                .addDeleteParameter("ignoreUnknownIds", true)
                .withItemMappingFunction(this::toItem)
                .withIdFunction(this::getRelationshipId);

        if (getClient().getClientConfig().getUpsertMode() == UpsertMode.REPLACE) {
            // Just delete and re-create the relationship since there is no internal id to take into account.
            return upsertItems.upsertViaDeleteAndCreate(relationships).stream()
                    .map(this::parseRelationship)
                    .collect(Collectors.toList());
        }

        return upsertItems.upsertViaCreateAndUpdate(relationships).stream()
                .map(this::parseRelationship)
                .collect(Collectors.toList());
    }

    /**
     * Deletes a set of Relationships.
     *
     * The events to delete are identified via their {@code externalId / id} by submitting a list of
     * {@link Item}.
     *
     * @param relationships a list of {@link Item} representing the events (externalId / id) to be deleted
     * @return The deleted events via {@link Item}
     * @throws Exception
     */
    public List<Item> delete(List<Item> relationships) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteRelationships();

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildAuthConfig())
                .addParameter("ignoreUnknownIds", true);

        return deleteItems.deleteItems(relationships);
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Relationship parseRelationship(String json) {
        try {
            return RelationshipParser.parseRelationship(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(Relationship item) {
        try {
            return RelationshipParser.toRequestInsertItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestUpdateItem(Relationship item) {
        try {
            return RelationshipParser.toRequestUpdateItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of an event. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getRelationshipId(Relationship item) {
        return Optional.of(item.getExternalId());
    }

    /*
    Returns an Item reflecting the input relationship. This will extract the relationship externalId
    and populate the Item with it.
     */
    private Item toItem(Relationship item) {
        return Item.newBuilder()
                .setExternalId(item.getExternalId())
                .build();
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract Relationships build();
    }
}
