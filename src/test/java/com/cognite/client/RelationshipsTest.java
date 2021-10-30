package com.cognite.client;

import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.*;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationshipsTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteRelationships() throws Exception {
        Instant startInstant = Instant.now();
        int noAssets = 50;
        //int noAssets = 5;
        int noEvents = 7895;
        //int noEvents = 2;
        int noTs = 2346;
        //int noTs = 2;
        int noSeq = 234;
        //int noSeq = 1;

        String loggingPrefix = "UnitTest - writeReadAndDeleteRelationships() -";
        LOG.info(loggingPrefix + "------------------- Start test. Creating Cognite client. ----------------------");
        CogniteClient client = CogniteClient.ofClientCredentials(
                    TestConfigProvider.getClientId(),
                    TestConfigProvider.getClientSecret(),
                    TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                    .withProject(TestConfigProvider.getProject())
                    .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "------------------- Finished creating the Cognite client. Duration : {} ------------------",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "------------------- Start upserting entities. --------------------");
            List<Asset> assetUpsertList = client
                    .assets()
                    .upsert(DataGenerator.generateAssetHierarchy(noAssets));
            List<Event> eventUpsertList = client
                    .events()
                    .upsert(DataGenerator.generateEvents(noEvents));
            List<TimeseriesMetadata> timseriesUpsertList = client
                    .timeseries()
                    .upsert(DataGenerator.generateTsHeaderObjects(noTs));
            List<SequenceMetadata> sequenceUpsertList = client
                    .sequences()
                    .upsert(DataGenerator.generateSequenceMetadata(noSeq));
            LOG.info(loggingPrefix + "-------------- Finished upserting entities. Duration: {} ------------------",
                    Duration.between(startInstant, Instant.now()));


            LOG.info(loggingPrefix + "------------------- Start upserting relationships. --------------------");
            List<Relationship> relationshipList = new ArrayList<>();
            for (Event event : eventUpsertList) {
                Relationship.Builder relBuilder = DataGenerator.generateRelationships(1).get(0).toBuilder();
                int randomAssetIndex = (int) Math.random() * assetUpsertList.size();
                // set the source and target references
                relBuilder
                        .setSourceType(Relationship.ResourceType.EVENT)
                        .setSourceExternalId(event.getExternalId())
                        .setTargetType(Relationship.ResourceType.ASSET)
                        .setTargetExternalId(assetUpsertList.get(randomAssetIndex).getExternalId());
                relationshipList.add(relBuilder.build());
            }
            for (TimeseriesMetadata ts : timseriesUpsertList) {
                Relationship.Builder relBuilder = DataGenerator.generateRelationships(1).get(0).toBuilder();
                int randomAssetIndex = (int) Math.random() * assetUpsertList.size();
                // set the source and target references
                relBuilder
                        .setSourceType(Relationship.ResourceType.TIME_SERIES)
                        .setSourceExternalId(ts.getExternalId())
                        .setTargetType(Relationship.ResourceType.ASSET)
                        .setTargetExternalId(assetUpsertList.get(randomAssetIndex).getExternalId());
                relationshipList.add(relBuilder.build());
            }
            for (SequenceMetadata sequence : sequenceUpsertList) {
                Relationship.Builder relBuilder = DataGenerator.generateRelationships(1).get(0).toBuilder();
                int randomEventIndex = (int) Math.random() * eventUpsertList.size();
                // set the source and target references
                relBuilder
                        .setSourceType(Relationship.ResourceType.SEQUENCE)
                        .setSourceExternalId(sequence.getExternalId())
                        .setTargetType(Relationship.ResourceType.EVENT)
                        .setTargetExternalId(eventUpsertList.get(randomEventIndex).getExternalId());
                relationshipList.add(relBuilder.build());
            }
            List<Relationship> upsertRelationshipsList = client.relationships()
                    .upsert(relationshipList);
            LOG.info(loggingPrefix + "-------------- Finished upserting relationships. Duration: {} ------------------",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "---------------- Start listing relationships. ---------------------------");
            List<Relationship> listRelationshipsResults = new ArrayList<>();
            client.relationships()
                    .list(Request.create()
                            .withRootParameter("fetchResources", true))
                    .forEachRemaining(relationships -> listRelationshipsResults.addAll(relationships));
            LOG.info(loggingPrefix + "----------------------- Finished listing relationships. Duration: {} -----------------",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "---------------- Start deleting relationships. ---------------------");
            List<Item> deleteItemsInput = new ArrayList<>();
            listRelationshipsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.relationships().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "----------------- Finished deleting relationships. Duration: {} -----------------",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "---------------- Start deleting entities. ---------------------");
            List<Item> deleteItemsEvents = eventUpsertList.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .collect(Collectors.toList());
            List<Item> deleteEventsResults = client.events().delete(deleteItemsEvents);

            List<Item> deleteItemsTs = timseriesUpsertList.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .collect(Collectors.toList());
            List<Item> deleteTsResults = client.timeseries().delete(deleteItemsTs);

            List<Item> deleteItemsSequences = sequenceUpsertList.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .collect(Collectors.toList());
            List<Item> deleteSequenceResults = client.sequences().delete(deleteItemsSequences);

            List<Item> deleteItemsAssets = assetUpsertList.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .collect(Collectors.toList());
            List<Item> deleteAssetsResults = client.assets().delete(deleteItemsAssets);

            LOG.info(loggingPrefix + "----------------- Finished deleting entities. Duration: {} -----------------",
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
    void writeEditAndDeleteRelationships() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeEditAndDeleteRelationships() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofClientCredentials(
                    TestConfigProvider.getClientId(),
                    TestConfigProvider.getClientSecret(),
                    TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                    .withProject(TestConfigProvider.getProject())
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
                                .setConfidence(1f)
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
                    if (relationship.getConfidence() > 0.99f) {
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
    void writeRetrieveAndDeleteRelationships() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeRetrieveAndDeleteRelationships() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofClientCredentials(
                    TestConfigProvider.getClientId(),
                    TestConfigProvider.getClientSecret(),
                    TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                    .withProject(TestConfigProvider.getProject())
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

            List<Relationship> retrievedRelationships = client.relationships().retrieve(relationshipItems, true);
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