package com.cognite.client;

import com.cognite.client.config.TokenUrl;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.Item;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.util.DataGenerator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class EventsTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteEvents() {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeReadAndDeleteEvents() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");


        try {
            /*
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
         */
            CogniteClient client = CogniteClient.ofClientCredentials(
                    TestConfigProvider.getClientId(),
                    TestConfigProvider.getClientSecret(),
                    TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                    .withBaseUrl(TestConfigProvider.getHost());
            LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start upserting events.");
            List<Event> upsertEventsList = DataGenerator.generateEvents(13800);
            client.events().upsert(upsertEventsList);
            LOG.info(loggingPrefix + "Finished upserting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            Thread.sleep(20000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading events.");
            List<Event> listEventsResults = new ArrayList<>();
            client.events()
                    .list(Request.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(events -> listEventsResults.addAll(events));
            LOG.info(loggingPrefix + "Finished reading events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start deleting events.");
            List<Item> deleteItemsInput = listEventsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .collect(Collectors.toList());

            List<Item> deleteItemsResults = client.events().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            assertEquals(upsertEventsList.size(), listEventsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeEditAndDeleteEvents() {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeEditAndDeleteEvents() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        try {
            LOG.info(loggingPrefix + "Start upserting events.");
            List<Event> upsertEventsList = DataGenerator.generateEvents(123);
            List<Event> upsertedEvents = client.events().upsert(upsertEventsList);
            LOG.info(loggingPrefix + "Finished upserting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            Thread.sleep(3000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start updating events.");
            List<Event> editedEventsInput = upsertedEvents.stream()
                    .map(event -> event.toBuilder()
                            .setDescription("new-value")
                            .clearSubtype()
                            .clearMetadata()
                            .putMetadata("new-key", "new-value")
                            .build())
                    .collect(Collectors.toList());

            List<Event> eventUpdateResults = client.events().upsert(editedEventsInput);
            LOG.info(loggingPrefix + "Finished updating events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start update replace events.");
            client = client
                    .withClientConfig(ClientConfig.create()
                            .withUpsertMode(UpsertMode.REPLACE));

            List<Event> eventReplaceResults = client.events().upsert(editedEventsInput);
            LOG.info(loggingPrefix + "Finished update replace events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            Thread.sleep(3000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading events.");
            List<Event> listEventsResults = new ArrayList<>();
            client.events()
                    .list(Request.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(events -> listEventsResults.addAll(events));
            LOG.info(loggingPrefix + "Finished reading events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start deleting events.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listEventsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.events().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            BooleanSupplier updateCondition = () -> {
                for (Event event : eventUpdateResults)  {
                    if (event.getDescription().equals("new-value")
                            && event.hasSubtype()
                            && event.containsMetadata("new-key")
                            && event.containsMetadata(DataGenerator.sourceKey)) {
                        // all good
                    } else {
                        return false;
                    }
                }
                return true;
            };

            BooleanSupplier replaceCondition = () -> {
                for (Event event : eventReplaceResults)  {
                    if (event.getDescription().equals("new-value")
                            && !event.hasSubtype()
                            && event.containsMetadata("new-key")
                            && !event.containsMetadata(DataGenerator.sourceKey)) {
                        // all good
                    } else {
                        return false;
                    }
                }
                return true;
            };

            assertTrue(updateCondition, "Event update not correct");
            assertTrue(replaceCondition, "Event replace not correct");

            assertEquals(upsertEventsList.size(), listEventsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeRetrieveAndDeleteEvents() {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDeleteEvents() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting events.");
            List<Event> upsertEventsList = DataGenerator.generateEvents(16800);
            client.events().upsert(upsertEventsList);
            LOG.info(loggingPrefix + "Finished upserting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start listing events.");
            List<Event> listEventsResults = new ArrayList<>();
            client.events()
                    .list(Request.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(events -> listEventsResults.addAll(events));
            LOG.info(loggingPrefix + "Finished listing events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start retrieving events.");
            List<Item> eventItems = new ArrayList<>();
            listEventsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .forEach(item -> eventItems.add(item));

            List<Event> retrievedEvents = client.events().retrieve(eventItems);
            LOG.info(loggingPrefix + "Finished retrieving events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting events.");
            List<Item> deleteItemsInput = new ArrayList<>();
            retrievedEvents.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.events().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertEventsList.size(), listEventsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
            assertEquals(eventItems.size(), retrievedEvents.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeAggregateAndDeleteEvents() {
        int noItems = 745;
        Instant startInstant = Instant.now();

        String loggingPrefix = "UnitTest - writeAggregateAndDeleteEvents() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting events.");
            List<Event> upsertEventsList = DataGenerator.generateEvents(noItems);
            client.events().upsert(upsertEventsList);
            LOG.info(loggingPrefix + "Finished upserting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(10000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start aggregating events.");
            Aggregate aggregateResult = client.events()
                    .aggregate(Request.create()
                            .withFilterParameter("source", DataGenerator.sourceValue));
            LOG.info(loggingPrefix + "Aggregate results: {}", aggregateResult);
            LOG.info(loggingPrefix + "Finished aggregating events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start reading events.");
            List<Event> listEventsResults = new ArrayList<>();
            client.events()
                    .list(Request.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(events -> listEventsResults.addAll(events));
            LOG.info(loggingPrefix + "Finished reading events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting events.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listEventsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.events().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertEventsList.size(), listEventsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

}