package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.Relationship;
import com.cognite.client.util.DataGenerator;
import com.google.protobuf.FloatValue;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationshipsTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteRelationships() {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeReadAndDeleteRelationships() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting relationships.");
            List<Relationship> upsertRelationshipsList = DataGenerator.generateRelationships(9876);
            client.relationships().upsert(upsertRelationshipsList);
            LOG.info(loggingPrefix + "Finished upserting relationships. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start listing relationships.");
            List<Relationship> listRelationshipsResults = new ArrayList<>();
            client.relationships()
                    .list(Request.create()
                            )
                    .forEachRemaining(relationships -> listRelationshipsResults.addAll(relationships));
            LOG.info(loggingPrefix + "Finished listing relationships. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting relationships.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listRelationshipsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.relationships().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting relationships. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertRelationshipsList.size(), listRelationshipsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeEditAndDeleteRelationships() {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeEditAndDeleteRelationships() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting relationships.");
            List<Relationship> upsertRelationshipsList = DataGenerator.generateRelationships(221);
            List<Relationship> upsertedRelationships = client.relationships().upsert(upsertRelationshipsList);
            LOG.info(loggingPrefix + "Finished upserting relationships. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(3000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start updating relationships.");
            List<Relationship> updatedRelationshipsInput = upsertedRelationships.stream()
                    .map(relationship ->
                        relationship.toBuilder()
                                .setConfidence(FloatValue.of(1f))
                                .build())
                    .collect(Collectors.toList());
            List<Relationship> updatedRelationshipsResult = client.relationships().upsert(updatedRelationshipsInput);

            LOG.info(loggingPrefix + "Finished updating relationships. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(3000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start deleting relationships.");
            List<Relationship> listRelationshipsResults = new ArrayList<>();
            client.relationships()
                    .list(Request.create())
                    .forEachRemaining(relationships -> listRelationshipsResults.addAll(relationships));

            List<Item> deleteItemsInput = new ArrayList<>();
            listRelationshipsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.relationships().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting relationships. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            BooleanSupplier updateCondition = () -> {
                for (Relationship relationship : updatedRelationshipsResult)  {
                    if (relationship.getConfidence().getValue() > 0.99f) {
                        // all good
                    } else {
                        return false;
                    }
                }
                return true;
            };

            assertTrue(updateCondition, "Relationship update not correct");
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
            assertEquals(upsertRelationshipsList.size(), listRelationshipsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeRetrieveAndDeleteRelationships() {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeRetrieveAndDeleteRelationships() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting relationships.");
            List<Relationship> upsertRelationshipsList = DataGenerator.generateRelationships(9876);
            client.relationships().upsert(upsertRelationshipsList);
            LOG.info(loggingPrefix + "Finished upserting relationships. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start listing relationships.");
            List<Relationship> listRelationshipsResults = new ArrayList<>();
            client.relationships()
                    .list(Request.create()
                    )
                    .forEachRemaining(relationships -> listRelationshipsResults.addAll(relationships));
            LOG.info(loggingPrefix + "Finished listing relationships. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start retrieving relationships.");
            List<Item> relationshipItems = new ArrayList<>();
            listRelationshipsResults.stream()
                    .map(relationship -> Item.newBuilder()
                            .setExternalId(relationship.getExternalId())
                            .build())
                    .forEach(item -> relationshipItems.add(item));

            List<Relationship> retrievedRelationships = client.relationships().retrieve(relationshipItems);
            LOG.info(loggingPrefix + "Finished retrieving relationships. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting relationships.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listRelationshipsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.relationships().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting relationships. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
            assertEquals(relationshipItems.size(), retrievedRelationships.size());
            assertEquals(upsertRelationshipsList.size(), listRelationshipsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }
}