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

import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.Item;
import com.cognite.client.config.ResourceType;
import com.cognite.client.queue.UpsertTarget;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.EventParser;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.stream.ListSource;
import com.cognite.client.stream.Publisher;
import com.cognite.client.util.Items;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite events api endpoint.
 * <p>
 * It provides methods for reading and writing {@link Event}.
 */
@AutoValue
public abstract class Events extends ApiBase implements ListSource<Event>, UpsertTarget<Event> {

    private static Builder builder() {
        return new AutoValue_Events.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(Events.class);

    /**
     * Constructs a new {@link Events} object using the provided client configuration.
     * <p>
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the events api object.
     */
    public static Events of(CogniteClient client) {
        return Events.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link Event} objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<Event> listResults = new ArrayList<>();
     *     client.events()
     *             .list()
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * @see #list(Request)
     * @see CogniteClient
     * @see CogniteClient#events()
     *
     */
    public Iterator<List<Event>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Returns all {@link Event} objects that matches the filters set in the {@link Request}.
     * <p>
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     * <p>
     * The events are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Event> listResults = new ArrayList<>();
     *      client.events()
     *              .list(Request.create()
     *                             .withFilterParameter("source", "source"))
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * @see #list(Request,String...)
     * @see CogniteClient
     * @see CogniteClient#events()
     *
     * @param requestParameters the filters to use for retrieving the events.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Event>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link Event} objects that matches the filters set in the {@link Request} for the
     * specified partitions. This is method is intended for advanced use cases where you need direct control over
     * the individual partitions. For example, when using the SDK in a distributed computing environment.
     * <p>
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Event> listResults = new ArrayList<>();
     *      client.events()
     *              .list(Request.create()
     *                             .withFilterParameter("source", "source"),
     *                                  "1/8","2/8","3/8","4/8","5/8","6/8","7/8","8/8")
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * @see #listJson(ResourceType,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#events()
     *
     * @param requestParameters the filters to use for retrieving the events.
     * @param partitions        the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Event>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.EVENT, requestParameters, partitions), this::parseEvent);
    }

    /**
     * Returns a {@link Publisher} that can stream {@link Event} from Cognite Data Fusion.
     *
     * When an {@link Event} is created or updated, it will be captured by the publisher and emitted to the registered
     * consumer.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Event> eventList = new CopyOnWriteArrayList<>();
     *      Publisher<Event> publisher = client.events().stream()
     *                     .withRequest(Request.create()
     *                             .withFilterMetadataParameter("source", "source"))
     *                     .withStartTime(Instant.now())
     *                     .withEndTime(Instant.now().plusSeconds(25))
     *                     .withPollingInterval(Duration.ofSeconds(2))
     *                     .withPollingOffset(Duration.ofSeconds(15L))
     *                     .withConsumer(batch -> {
     *                         eventList.addAll(batch);
     *                     });
     *      Future<Boolean> streamer = publisher.start();
     *      Boolean result = streamer.get();
     * }
     * </pre>
     *
     * @see CogniteClient
     * @see CogniteClient#events()
     * @see java.util.concurrent.CopyOnWriteArrayList
     *
     * @return The publisher producing the stream of events. Call {@code start()} to start the stream.
     */
    public Publisher<Event> stream() {
        return Publisher.of(this);
    }

    /**
     * Retrieve events by {@code externalId}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Event> retrievedEvent = client.events().retrieve("1","2");
     * }
     * </pre>
     *
     * @see #retrieve(List)
     * @see CogniteClient
     * @see CogniteClient#events()
     *
     * @param externalId The {@code externalIds} to retrieve
     * @return The retrieved events.
     * @throws Exception
     */
    public List<Event> retrieve(String... externalId) throws Exception {
        return retrieve(Items.parseItems(externalId));
    }

    /**
     * Retrieve events by {@code internal id}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Event> retrievedEvent = client.events().retrieve(1,2);
     * }
     * </pre>
     *
     * @see #retrieve(List)
     * @see CogniteClient
     * @see CogniteClient#events()
     *
     * @param id The {@code ids} to retrieve
     * @return The retrieved events.
     * @throws Exception
     */
    public List<Event> retrieve(long... id) throws Exception {
        return retrieve(Items.parseItems(id));
    }

    /**
     * Retrieve events by {@code externalId / id}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Item> items = List.of(Item.newBuilder().setExternalId("1").build());
     *      List<Event> retrievedEvents = client.events().retrieve(items);
     * }
     * </pre>
     *
     * @see #retrieveJson(ResourceType,Collection)
     * @see CogniteClient
     * @see CogniteClient#events()
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved events.
     * @throws Exception
     */
    public List<Event> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.EVENT, items).stream()
                .map(this::parseEvent)
                .collect(Collectors.toList());
    }

    /**
     * Performs an item aggregation request to Cognite Data Fusion.
     * <p>
     * The default aggregation is a total item count based on the (optional) filters in the request.
     * Multiple aggregation types are supported. Please refer to the Cognite API specification for more information
     * on the possible settings.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Aggregate aggregateResult = client.events()
     *                  .aggregate(Request.create()
     *                  .withFilterParameter("source", "source"));
     * }
     * </pre>
     *
     * @see #aggregate(ResourceType,Request)
     * @see CogniteClient
     * @see CogniteClient#events()
     *
     * @param requestParameters The filtering and aggregates specification
     * @return The aggregation results.
     * @throws Exception
     * @see <a href="https://docs.cognite.com/api/v1/">Cognite API v1 specification</a>
     */
    public Aggregate aggregate(Request requestParameters) throws Exception {
        return aggregate(ResourceType.EVENT, requestParameters);
    }

    /**
     * Creates or updates a set of {@link Event} objects.
     * <p>
     * If it is a new {@link Event} object (based on {@code id / externalId}, then it will be created.
     * <p>
     * If an {@link Event} object already exists in Cognite Data Fusion, it will be updated. The update behavior
     * is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Event> events = // List of Events;
     *      client.events().upsert(events);
     * }
     * </pre>
     *
     * @see UpsertItems#upsertViaCreateAndUpdate(List)
     * @see CogniteClient
     * @see CogniteClient#events()
     *
     * @param events The events to upsert.
     * @return The upserted events.
     * @throws Exception
     */
    public List<Event> upsert(List<Event> events) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeEvents();
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateEvents();

        UpsertItems<Event> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withUpdateItemWriter(updateItemWriter)
                .withUpdateMappingFunction(this::toRequestUpdateItem)
                .withIdFunction(this::getEventId);

        if (getClient().getClientConfig().getUpsertMode() == UpsertMode.REPLACE) {
            upsertItems = upsertItems.withUpdateMappingFunction(this::toRequestReplaceItem);
        }

        return upsertItems.upsertViaCreateAndUpdate(events).stream()
                .map(this::parseEvent)
                .collect(Collectors.toList());
    }

    /**
     * Deletes a set of Events.
     * <p>
     * The events to delete are identified via their {@code externalId / id} by submitting a list of
     * {@link Item}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<Item> events = List.of(Item.newBuilder().setExternalId("1").build());
     *     List<Item> deletedItemsResults = client.events().delete(events);
     * }
     * </pre>
     *
     * @see DeleteItems#deleteItems(List)
     * @see CogniteClient
     * @see CogniteClient#events()
     *
     * @param events a list of {@link Item} representing the events (externalId / id) to be deleted
     * @return The deleted events via {@link Item}
     * @throws Exception
     */
    public List<Item> delete(List<Item> events) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteEvents();

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildAuthConfig())
                .addParameter("ignoreUnknownIds", true);

        return deleteItems.deleteItems(events);
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Event parseEvent(String json) {
        try {
            return EventParser.parseEvent(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(Event item) {
        try {
            return EventParser.toRequestInsertItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestUpdateItem(Event item) {
        try {
            return EventParser.toRequestUpdateItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestReplaceItem(Event item) {
        try {
            return EventParser.toRequestReplaceItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of an event. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getEventId(Event item) {
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
        abstract Events build();
    }
}
