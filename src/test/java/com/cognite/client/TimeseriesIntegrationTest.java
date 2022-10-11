package com.cognite.client;

import com.amazonaws.services.s3.transfer.Upload;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.*;
import com.cognite.client.queue.UploadQueue;
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

class TimeseriesIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteTimeseries() throws Exception {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeReadAndDeleteTimeseries() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofClientCredentials(
                    TestConfigProvider.getClientId(),
                    TestConfigProvider.getClientSecret(),
                    TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                    .withProject(TestConfigProvider.getProject())
                    .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start upserting timeseries.");
        List<TimeseriesMetadata> upsertTimeseriesList = DataGenerator.generateTsHeaderObjects(7800);
        client.timeseries().upsert(upsertTimeseriesList);
        LOG.info(loggingPrefix + "Finished upserting timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        Thread.sleep(20000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start reading timeseries.");
        List<TimeseriesMetadata> listTimeseriesResults = new ArrayList<>();
        client.timeseries()
                .list(Request.create()
                        .withFilterMetadataParameter("source", DataGenerator.sourceValue))
                .forEachRemaining(timeseries -> listTimeseriesResults.addAll(timeseries));
        LOG.info(loggingPrefix + "Finished reading timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start deleting timeseries.");
        List<Item> deleteItemsInput = new ArrayList<>();
        listTimeseriesResults.stream()
                .map(timeseries -> Item.newBuilder()
                        .setExternalId(timeseries.getExternalId())
                        .build())
                .forEach(item -> deleteItemsInput.add(item));

        List<Item> deleteItemsResults = client.timeseries().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        assertEquals(upsertTimeseriesList.size(), listTimeseriesResults.size());
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
    }

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteTimeseriesDataPoints() throws Exception {
        Instant startInstant = Instant.now();
        final int noTsHeaders = 15;
        final int noTsPoints = 517893;
        final double tsPointsFrequency = 1d;
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(4)
                .withNoListPartitions(4);
        String loggingPrefix = "UnitTest - writeReadAndDeleteTimeseriesDataPoints() -";
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofClientCredentials(
                    TestConfigProvider.getClientId(),
                    TestConfigProvider.getClientSecret(),
                    TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                    .withProject(TestConfigProvider.getProject())
                    .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start upserting timeseries.");
        List<TimeseriesMetadata> upsertTimeseriesList = DataGenerator.generateTsHeaderObjects(noTsHeaders);
        client.timeseries().upsert(upsertTimeseriesList);
        LOG.info(loggingPrefix + "Finished upserting timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        Thread.sleep(5000); // wait for eventual consistency
        LOG.info(loggingPrefix + "Start upserting data points.");
        List<TimeseriesPointPost> upsertDataPointsList = DataGenerator.generateTsDatapointsObjects(
                noTsPoints,
                tsPointsFrequency,
                upsertTimeseriesList.stream()
                        .map(header -> header.getExternalId())
                        .collect(Collectors.toList()));
        client.timeseries().dataPoints().upsert(upsertDataPointsList);
        LOG.info(loggingPrefix + "Finished upserting data points. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        Thread.sleep(5000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start reading timeseries.");
        List<TimeseriesMetadata> listTimeseriesResults = new ArrayList<>();
        client.timeseries()
                .list(Request.create()
                        .withFilterMetadataParameter("source", DataGenerator.sourceValue))
                .forEachRemaining(timeseries -> listTimeseriesResults.addAll(timeseries));
        LOG.info(loggingPrefix + "Finished reading timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start reading data points.");
        List<Item> listDataPointsItems = upsertTimeseriesList.stream()
                .map(header -> Item.newBuilder()
                        .setExternalId(header.getExternalId())
                        .build())
                .collect(Collectors.toList());
        List<TimeseriesPoint> readDataPointsResults = new ArrayList<>();
        client.timeseries().dataPoints().retrieveComplete(listDataPointsItems)
                .forEachRemaining(timeseries -> readDataPointsResults.addAll(timeseries));
        LOG.info(loggingPrefix + "Finished reading data points. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start deleting timeseries.");
        List<Item> deleteItemsInput = new ArrayList<>();
        listTimeseriesResults.stream()
                .map(timeseries -> Item.newBuilder()
                        .setExternalId(timeseries.getExternalId())
                        .build())
                .forEach(item -> deleteItemsInput.add(item));

        List<Item> deleteItemsResults = client.timeseries().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        assertEquals(upsertDataPointsList.size(), readDataPointsResults.size());
        assertEquals(upsertTimeseriesList.size(), listTimeseriesResults.size());
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
    }

    @Test
    @Tag("remoteCDP")
    void writeEditAndDeleteTimeseries() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeEditAndDeleteTimeseries() -";
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

        LOG.info(loggingPrefix + "Start upserting timeseries.");
        List<TimeseriesMetadata> upsertTimeseriesList = DataGenerator.generateTsHeaderObjects(123);
        List<TimeseriesMetadata> upsertedTimeseries = client.timeseries().upsert(upsertTimeseriesList);
        LOG.info(loggingPrefix + "Finished upserting timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        Thread.sleep(3000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start updating timeseries.");
        List<TimeseriesMetadata> editedTimeseriesInput = upsertedTimeseries.stream()
                .map(timeseries -> timeseries.toBuilder()
                        .setDescription("new-value")
                        .clearMetadata()
                        .putMetadata("new-key", "new-value")
                        .build())
                .collect(Collectors.toList());

        List<TimeseriesMetadata> timeseriesUpdateResults = client.timeseries().upsert(editedTimeseriesInput);
        LOG.info(loggingPrefix + "Finished updating timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start update replace timeseries.");
        client = client
                .withClientConfig(ClientConfig.create()
                        .withUpsertMode(UpsertMode.REPLACE));

        List<TimeseriesMetadata> timeseriesReplaceResults = client.timeseries().upsert(editedTimeseriesInput);
        LOG.info(loggingPrefix + "Finished update replace timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        Thread.sleep(3000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start reading timeseries.");
        List<TimeseriesMetadata> listTimeseriesResults = new ArrayList<>();
        client.timeseries()
                .list(Request.create()
                        .withFilterMetadataParameter("new-key", "new-value"))
                .forEachRemaining(timeseries -> listTimeseriesResults.addAll(timeseries));
        LOG.info(loggingPrefix + "Finished reading timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start deleting timeseries.");
        List<Item> deleteItemsInput = new ArrayList<>();
        listTimeseriesResults.stream()
                .map(event -> Item.newBuilder()
                        .setExternalId(event.getExternalId())
                        .build())
                .forEach(item -> deleteItemsInput.add(item));

        List<Item> deleteItemsResults = client.timeseries().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        BooleanSupplier updateCondition = () -> {
            for (TimeseriesMetadata timeseries : timeseriesUpdateResults)  {
                if (timeseries.getDescription().equals("new-value")
                        && timeseries.containsMetadata("new-key")
                        && timeseries.containsMetadata(DataGenerator.sourceKey)) {
                    // all good
                } else {
                    return false;
                }
            }
            return true;
        };

        BooleanSupplier replaceCondition = () -> {
            for (TimeseriesMetadata timeseries : timeseriesReplaceResults)  {
                if (timeseries.getDescription().equals("new-value")
                        && timeseries.containsMetadata("new-key")
                        && !timeseries.containsMetadata(DataGenerator.sourceKey)) {
                    // all good
                } else {
                    return false;
                }
            }
            return true;
        };

        assertTrue(updateCondition, "Timeseries update not correct");
        assertTrue(replaceCondition, "Timeseries replace not correct");

        assertEquals(upsertTimeseriesList.size(), listTimeseriesResults.size());
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
    }

    @Test
    @Tag("remoteCDP")
    void writeRetrieveAndDeleteTimeseries() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDeleteTimeseries() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofClientCredentials(
                    TestConfigProvider.getClientId(),
                    TestConfigProvider.getClientSecret(),
                    TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                    .withProject(TestConfigProvider.getProject())
                    .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start upserting timeseries.");
        List<TimeseriesMetadata> upsertTimeseriesList = DataGenerator.generateTsHeaderObjects(16800);
        client.timeseries().upsert(upsertTimeseriesList);
        LOG.info(loggingPrefix + "Finished upserting timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        Thread.sleep(15000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start listing timeseries.");
        List<TimeseriesMetadata> listTimeseriesResults = new ArrayList<>();
        client.timeseries()
                .list(Request.create()
                        .withFilterMetadataParameter("source", DataGenerator.sourceValue))
                .forEachRemaining(timeseries -> listTimeseriesResults.addAll(timeseries));
        LOG.info(loggingPrefix + "Finished listing timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start retrieving timeseries.");
        List<Item> timeseriesItems = new ArrayList<>();
        listTimeseriesResults.stream()
                .map(timeseries -> Item.newBuilder()
                        .setExternalId(timeseries.getExternalId())
                        .build())
                .forEach(item -> timeseriesItems.add(item));

        List<TimeseriesMetadata> retrievedTimeseries = client.timeseries().retrieve(timeseriesItems);
        LOG.info(loggingPrefix + "Finished retrieving timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start deleting timeseries.");
        List<Item> deleteItemsInput = new ArrayList<>();
        retrievedTimeseries.stream()
                .map(timeseries -> Item.newBuilder()
                        .setExternalId(timeseries.getExternalId())
                        .build())
                .forEach(item -> deleteItemsInput.add(item));

        List<Item> deleteItemsResults = client.timeseries().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        assertEquals(upsertTimeseriesList.size(), listTimeseriesResults.size());
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        assertEquals(timeseriesItems.size(), retrievedTimeseries.size());
    }

    @Test
    @Tag("remoteCDP")
    void writeAggregateAndDeleteTimeseries() throws Exception {
        int noItems = 745;
        Instant startInstant = Instant.now();

        String loggingPrefix = "UnitTest - writeAggregateAndDeleteTimeseries() -";
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

        LOG.info(loggingPrefix + "Start upserting timeseries.");
        List<TimeseriesMetadata> upsertTimeseriesList = DataGenerator.generateTsHeaderObjects(noItems);
        client.timeseries().upsert(upsertTimeseriesList);
        LOG.info(loggingPrefix + "Finished upserting timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        Thread.sleep(10000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start aggregating timeseries.");
        Aggregate aggregateResult = client.timeseries()
                .aggregate(Request.create()
                        .withFilterMetadataParameter("source", DataGenerator.sourceValue));
        LOG.info(loggingPrefix + "Aggregate results: {}", aggregateResult);
        LOG.info(loggingPrefix + "Finished aggregating timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start reading timeseries.");
        List<TimeseriesMetadata> listTimeseriesResults = new ArrayList<>();
        client.timeseries()
                .list(Request.create()
                        .withFilterMetadataParameter("source", DataGenerator.sourceValue))
                .forEachRemaining(timeseries -> listTimeseriesResults.addAll(timeseries));
        LOG.info(loggingPrefix + "Finished reading timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start deleting timeseries.");
        List<Item> deleteItemsInput = new ArrayList<>();
        listTimeseriesResults.stream()
                .map(timeseries -> Item.newBuilder()
                        .setExternalId(timeseries.getExternalId())
                        .build())
                .forEach(item -> deleteItemsInput.add(item));

        List<Item> deleteItemsResults = client.timeseries().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        assertEquals(upsertTimeseriesList.size(), listTimeseriesResults.size());
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
    }

    @Test
    @Tag("remoteCDP")
    void writeUploadQueueAndDeleteTimeseriesDataPoints() throws Exception {
        Instant startInstant = Instant.now();
        final int noTsHeaders = 5;
        final int noTsPoints = 51789;
        final double tsPointsFrequency = 1d;
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(4)
                .withNoListPartitions(4);
        String loggingPrefix = "UnitTest - writeUploadQueueAndDeleteTimeseriesDataPoints() -";
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofClientCredentials(
                        TestConfigProvider.getClientId(),
                        TestConfigProvider.getClientSecret(),
                        TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                .withProject(TestConfigProvider.getProject())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start upserting timeseries.");
        List<TimeseriesMetadata> upsertTimeseriesList = DataGenerator.generateTsHeaderObjects(noTsHeaders);
        client.timeseries().upsert(upsertTimeseriesList);
        LOG.info(loggingPrefix + "Finished upserting timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        Thread.sleep(5000); // wait for eventual consistency
        LOG.info(loggingPrefix + "Start upserting data points.");
        List<TimeseriesPointPost> upsertDataPointsList = DataGenerator.generateTsDatapointsObjects(
                noTsPoints,
                tsPointsFrequency,
                upsertTimeseriesList.stream()
                        .map(header -> header.getExternalId())
                        .collect(Collectors.toList()));
        UploadQueue<TimeseriesPointPost, TimeseriesPointPost> uploadQueue = client.timeseries().dataPoints().uploadQueue()
                .withMaxUploadInterval(Duration.ofSeconds(1))
                .withPostUploadFunction(events -> LOG.info("postUploadFunction triggered. Uploaded {} items", events.size()))
                .withExceptionHandlerFunction(exception -> LOG.warn("exceptionHandlerFunction triggered: {}", exception.getMessage()));

        for (TimeseriesPointPost dataPoint : upsertDataPointsList) {
            uploadQueue.put(dataPoint);
        }
        uploadQueue.upload();

        LOG.info(loggingPrefix + "Finished upserting data points. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        Thread.sleep(5000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start reading timeseries.");
        List<TimeseriesMetadata> listTimeseriesResults = new ArrayList<>();
        client.timeseries()
                .list(Request.create()
                        .withFilterMetadataParameter("source", DataGenerator.sourceValue))
                .forEachRemaining(timeseries -> listTimeseriesResults.addAll(timeseries));
        LOG.info(loggingPrefix + "Finished reading timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start reading data points.");
        List<Item> listDataPointsItems = upsertTimeseriesList.stream()
                .map(header -> Item.newBuilder()
                        .setExternalId(header.getExternalId())
                        .build())
                .collect(Collectors.toList());
        List<TimeseriesPoint> readDataPointsResults = new ArrayList<>();
        client.timeseries().dataPoints().retrieveComplete(listDataPointsItems)
                .forEachRemaining(timeseries -> readDataPointsResults.addAll(timeseries));
        LOG.info(loggingPrefix + "Finished reading data points. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start deleting timeseries.");
        List<Item> deleteItemsInput = new ArrayList<>();
        listTimeseriesResults.stream()
                .map(timeseries -> Item.newBuilder()
                        .setExternalId(timeseries.getExternalId())
                        .build())
                .forEach(item -> deleteItemsInput.add(item));

        List<Item> deleteItemsResults = client.timeseries().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting timeseries. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        assertEquals(upsertDataPointsList.size(), readDataPointsResults.size());
        assertEquals(upsertTimeseriesList.size(), listTimeseriesResults.size());
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
    }
}