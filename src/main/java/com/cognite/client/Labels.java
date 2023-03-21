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
import com.cognite.client.dto.Label;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.LabelParser;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite labels api endpoint.
 *
 * It provides methods for reading and writing {@link Label}
 */
@AutoValue
public abstract class Labels extends ApiBase {

    private static Builder builder() {
        return new AutoValue_Labels.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(Labels.class);

    /**
     * Construct a new {@link Labels} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient} as the entry point
     * to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return The labels api object.
     */
    public static Labels of(CogniteClient client) {
        return Labels.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link Label} objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<Label> listResults = new ArrayList<>();
     *     client.labels()
     *             .list()
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Labels/operation/listLabels">API Reference - Filter labels</a>
     *
     * @see #list(Request)
     * @see CogniteClient
     * @see CogniteClient#labels()
     */
    public Iterator<List<Label>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Return all {@link Label} object that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, the you have
     * to stream these results into your own data structure.
     *
     * The labels are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Label> listResults = new ArrayList<>();
     *      client.labels()
     *              .list(Request.create()
     *                             .withFilterParameter("externalIdPrefix", "Val"))
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Labels/operation/listLabels">API Reference - Filter labels</a>
     *
     * @see #list(Request,String...)
     * @see CogniteClient
     * @see CogniteClient#labels()
     *
     * @param requestParameters The filters to use for retrieving labels.
     * @return An {@link Iterator} to page through the rsults set.
     * @throws Exception
     */
    public Iterator<List<Label>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link Label} objects that matches the filters set in the {@link Request} for the
     * specific partitions. This method is intended for advanced use cases where you need direct control over the
     * individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you neeed to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Label> listResults = new ArrayList<>();
     *      client.labels()
     *              .list(Request.create()
     *                             .withFilterParameter("externalIdPrefix", "Val"),
     *                                  "1/8","2/8","3/8","4/8","5/8","6/8","7/8","8/8")
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Labels/operation/listLabels">API Reference - Filter labels</a>
     *
     * @see #listJson(ResourceType,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#labels()
     *
     * @param requestParameters The filters to use for retrieving the labels
     * @param partitions The partitions to include.
     * @return An {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Label>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.LABEL, requestParameters, partitions), this::parseLabels);
    }

    /**
     * Creates or updates a set of {@link Label} objects.
     *
     * If it is a new {@link Label} object already exists in Cognite Data Fusion, it will be updated. The update
     * behaviour is specified via the update mode in teh {@link com.cognite.client.config.ClientConfig} settings.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Label> labels = // List of Labels;
     *      client.labels().upsert(labels);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Labels/operation/createLabelDefinitions">API Reference - Create label definitions.</a><br/>
     * <a href="https://docs.cognite.com/api/v1/#tag/Labels/operation/deleteLabels">API Reference - Delete label definitions.</a>
     *
     * @see UpsertItems#upsertViaCreateAndDelete(List)
     * @see CogniteClient
     * @see CogniteClient#labels()
     *
     * @param labels The labels to upsert.
     * @return The upserted labels
     * @throws Exception
     */
    public List<Label> upsert(List<Label> labels) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWrite = connector.writeLabels();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteLabels();

        UpsertItems<Label> upsertItems = UpsertItems.of(createItemWrite, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withDeleteItemWriter(deleteItemWriter)
                .withItemMappingFunction(this::toItem)
                .withIdFunction(this::getLabelId);

        return upsertItems.upsertViaCreateAndDelete(labels).stream()
                .map(this::parseLabels)
                .collect(Collectors.toList());
    }

    /**
     * Deletes a set of Labels.
     *
     * The labels to delete are identified via their {@code externalId / id} by submitting a list of {@link Item}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<Item> labels = List.of(Item.newBuilder().setExternalId("1").build());
     *     List<Item> deletedItemsResults = client.labels().delete(labels);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Labels/operation/deleteLabels">API Reference - Delete label definitions.</a>
     *
     * @see DeleteItems#deleteItems(List)
     * @see CogniteClient
     * @see CogniteClient#labels()
     *
     * @param labels A list of {@link Item} representing the labels (externalId / id) to be deleted.
     * @return The deleted labels via {@link Item}.
     * @throws Exception
     */
    public List<Item> delete(List<Item> labels) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteLabels();

        DeleteItems deleteItems = DeleteItems.ofItem(deleteItemWriter, getClient().buildAuthConfig());

        return deleteItems.deleteItems(labels);
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Label parseLabels(String json) {
        try {
            return LabelParser.parseLabel(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(Label item) {
        try {
            return LabelParser.toRequestInsertItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns an item reflecting the input label. This will extract the label externalId and populate the Item with it.
     */
    private Item toItem(Label item) {
        return Item.newBuilder()
                .setExternalId(item.getExternalId())
                .build();
    }

    /*
    Returns the externalId of a label.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getLabelId(Label item) {
        try {
            return Optional.of(item.getExternalId());
        } catch (Exception e) {
            return Optional.<String>empty();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract Labels build();
    }
}
