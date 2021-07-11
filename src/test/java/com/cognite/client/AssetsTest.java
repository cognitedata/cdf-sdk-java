package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.Asset;
import com.cognite.client.dto.Item;
import com.cognite.client.util.DataGenerator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AssetsTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteAssets() {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDeleteAssets() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting assets.");
            List<Asset> upsertAssetsList = DataGenerator.generateAssetHierarchy(1100);
            upsertAssetsList.addAll(DataGenerator.generateAssetHierarchy(1100));
            client.assets().upsert(upsertAssetsList);
            LOG.info(loggingPrefix + "Finished upserting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading assets.");
            List<Asset> listAssetsResults = new ArrayList<>();
            client.assets()
                    .list(Request.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(assets -> listAssetsResults.addAll(assets));
            LOG.info(loggingPrefix + "Finished reading assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting assets.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listAssetsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.assets().delete(deleteItemsInput, true);
            LOG.info(loggingPrefix + "Finished deleting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertAssetsList.size(), listAssetsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void synchronizeAssetHierarchy() {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - synchronizeAssetHierarchy() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start first synch assets.");
            List<Asset> originalAssetList = DataGenerator.generateAssetHierarchy(50);
            List<Asset> upsertedAssets = client.assets().synchronizeHierarchy(originalAssetList);
            LOG.info(loggingPrefix + "Finished first sync assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(2000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start second sync assets.");
            List<Asset> editedAssetsInput = originalAssetList.stream()
                    .map(asset -> {
                        if (ThreadLocalRandom.current().nextBoolean()) {
                            return asset.toBuilder()
                                    .putMetadata("new-key", "new-value")
                                    .build();
                        } else {
                            return asset;
                        }
                    })
                    .collect(Collectors.toList());

            List<Asset> assetsToRemove = identifyLeafAssets(editedAssetsInput).stream()
                    .limit(10)
                    .collect(Collectors.toList());

            editedAssetsInput.removeAll(assetsToRemove);
            client.assets().synchronizeHierarchy(editedAssetsInput);

            LOG.info(loggingPrefix + "Finished second sync assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(2000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start deleting assets.");
            List<Asset> listAssetsResults = new ArrayList<>();
            client.assets()
                    .list(Request.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(events -> listAssetsResults.addAll(events));
            List<Item> deleteItemsInput = new ArrayList<>();
            listAssetsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.assets().delete(deleteItemsInput, true);
            LOG.info(loggingPrefix + "Finished deleting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(editedAssetsInput.size(), listAssetsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeEditAndDeleteAssets() {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeEditAndDeleteAssets() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting assets.");
            List<Asset> upsertAssetsList = DataGenerator.generateAssetHierarchy(15);
            List<Asset> upsertedAssets = client.assets().upsert(upsertAssetsList);
            LOG.info(loggingPrefix + "Finished upserting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(2000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start updating assets.");
            List<Asset> editedAssetsInput = upsertedAssets.stream()
                    .map(asset -> asset.toBuilder()
                            .clearDescription()
                            .clearMetadata()
                            .putMetadata("new-key", "new-value")
                            .build())
                    .collect(Collectors.toList());

            List<Asset> assetUpdateResults = client.assets().upsert(editedAssetsInput);
            LOG.info(loggingPrefix + "Finished updating assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start update replace assets.");
            client = client
                    .withClientConfig(ClientConfig.create()
                            .withUpsertMode(UpsertMode.REPLACE));

            List<Asset> assetReplaceResults = client.assets().upsert(editedAssetsInput);
            LOG.info(loggingPrefix + "Finished update replace assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(2000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start deleting events.");
            List<Asset> listAssetsResults = new ArrayList<>();
            client.assets()
                    .list(Request.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(events -> listAssetsResults.addAll(events));
            List<Item> deleteItemsInput = new ArrayList<>();
            listAssetsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.assets().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            BooleanSupplier updateCondition = () -> {
                for (Asset asset : assetUpdateResults)  {
                    if (asset.hasDescription()
                            && asset.containsMetadata("new-key")
                            && asset.containsMetadata(DataGenerator.sourceKey)) {
                        // all good
                    } else {
                        return false;
                    }
                }
                return true;
            };

            BooleanSupplier replaceCondition = () -> {
                for (Asset asset : assetReplaceResults)  {
                    if (!asset.hasDescription()
                            && asset.containsMetadata("new-key")
                            && !asset.containsMetadata(DataGenerator.sourceKey)) {
                        // all good
                    } else {
                        return false;
                    }
                }
                return true;
            };

            assertTrue(updateCondition, "Asset update not correct");
            assertTrue(replaceCondition, "Asset replace not correct");
            assertEquals(upsertAssetsList.size(), listAssetsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeRetrieveAndDeleteAssets() {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeRetrieveAndDeleteAssets() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting assets.");
            List<Asset> upsertAssetsList = DataGenerator.generateAssetHierarchy(1680);
            client.assets().upsert(upsertAssetsList);
            LOG.info(loggingPrefix + "Finished upserting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start listing assets.");
            List<Asset> listAssetsResults = new ArrayList<>();
            client.assets()
                    .list(Request.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(events -> listAssetsResults.addAll(events));
            LOG.info(loggingPrefix + "Finished listing assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start retrieving assets.");
            List<Item> assetItems = new ArrayList<>();
            listAssetsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId().getValue())
                            .build())
                    .forEach(item -> assetItems.add(item));

            List<Asset> retrievedAssets = client.assets().retrieve(assetItems);
            LOG.info(loggingPrefix + "Finished retrieving assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting events.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listAssetsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.assets().delete(deleteItemsInput, true);
            LOG.info(loggingPrefix + "Finished deleting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertAssetsList.size(), listAssetsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
            assertEquals(assetItems.size(), retrievedAssets.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeAggregateAndDeleteAssets() {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeAggregateAndDeleteAssets() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting assets.");
            List<Asset> upsertAssetsList = DataGenerator.generateAssetHierarchy(1680);
            client.assets().upsert(upsertAssetsList);
            LOG.info(loggingPrefix + "Finished upserting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start aggregating assets.");
            Aggregate aggregateResult = client.assets()
                    .aggregate(Request.create()
                            .withFilterParameter("source", DataGenerator.sourceValue));
            LOG.info(loggingPrefix + "Aggregate results: {}", aggregateResult);
            LOG.info(loggingPrefix + "Finished aggregating assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start listing assets.");
            List<Asset> listAssetsResults = new ArrayList<>();
            client.assets()
                    .list(Request.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(events -> listAssetsResults.addAll(events));
            LOG.info(loggingPrefix + "Finished listing assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting assets.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listAssetsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.assets().delete(deleteItemsInput, true);
            LOG.info(loggingPrefix + "Finished deleting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertAssetsList.size(), listAssetsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    /*
    Return the leaf asset nodes of an asset collection.
     */
    private List<Asset> identifyLeafAssets(Collection<Asset> assetCollection) {
        List<String> parentRefs = assetCollection.stream()
                .filter(Asset::hasParentExternalId)
                .map(asset -> asset.getParentExternalId().getValue())
                .collect(Collectors.toList());

        List<Asset> leafNodes = assetCollection.stream()
                .filter(asset -> !parentRefs.contains(asset.getExternalId().getValue()))
                .collect(Collectors.toList());

        return leafNodes;
    }
}