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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExtractionPipelinesIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteExtractionPipelines() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDeleteExtractionPipelines() -";
        LOG.info(loggingPrefix + "---------------- Start test. Creating Cognite client. --------------------");

        CogniteClient client = CogniteClient.ofClientCredentials(
                TestConfigProvider.getClientId(),
                TestConfigProvider.getClientSecret(),
                TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                .withProject(TestConfigProvider.getProject())
                .withBaseUrl(TestConfigProvider.getHost());
        LOG.info(loggingPrefix + "-------------- Finished creating the Cognite client. Duration : {} ---------------",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "------------ Start create data set. ------------------");
        List<DataSet> dataSets = new ArrayList<>();
        client.datasets()
                .list(Request.create()
                        .withFilterMetadataParameter(DataGenerator.sourceKey, DataGenerator.sourceValue)
                )
                .forEachRemaining(dataSets::addAll);
        if (dataSets.isEmpty()) {
            List<DataSet> upsertDataSetList = DataGenerator.generateDataSets(1);
            dataSets = client.datasets().upsert(upsertDataSetList);
        }
        long dataSetId = dataSets.get(0).getId();
        LOG.info(loggingPrefix + "----------- Finished upserting data set. Duration: {} -------------",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "------------ Start upserting extraction pipelines. ------------------");
        List<ExtractionPipeline> upsertPipelinesList = DataGenerator.generateExtractionPipelines(3, dataSetId);
        client.extractionPipelines().upsert(upsertPipelinesList);
        LOG.info(loggingPrefix + "------------ Finished upserting extraction pipelines. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));

        Thread.sleep(2000); // wait for eventual consistency

        LOG.info(loggingPrefix + "------------ Start reading extraction pipelines. ------------------");
        List<ExtractionPipeline> listPipelinesResults = new ArrayList<>();
        client.extractionPipelines()
                .list(Request.create()
                        .withFilterParameter("source", DataGenerator.sourceValue)
                )
                .forEachRemaining(events -> listPipelinesResults.addAll(events));
        LOG.info(loggingPrefix + "------------ Finished reading extraction pipelines. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "------------ Start creating extraction pipeline runs. ------------------");
        List<ExtractionPipelineRun> upsertPipelineRunsList = new ArrayList<>();
        listPipelinesResults.stream()
                .map(extractionPipeline -> extractionPipeline.getExternalId())
                .forEach(extId -> upsertPipelineRunsList.addAll(DataGenerator.generateExtractionPipelineRuns(3, extId)));

        client.extractionPipelines().runs().create(upsertPipelineRunsList);
        LOG.info(loggingPrefix + "------------ Finished upserting extraction pipeline runs. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "------------ Start creating extraction pipeline heartbeats . ------------------");
        ExtractionPipelineRuns.Heartbeat heartbeat = client.extractionPipelines()
                .runs()
                .heartbeat(listPipelinesResults.get(0).getExternalId())
                .withInterval(Duration.ofSeconds(2));
        heartbeat.sendHeartbeat();
        heartbeat.start();
        Thread.sleep(5000);
        heartbeat.stop();
        client.extractionPipelines()
                .runs()
                .list(listPipelinesResults.get(0).getExternalId())
                .forEachRemaining(runs -> runs.stream().forEach(run -> LOG.info("Pipeline run: {}", run)));

        LOG.info(loggingPrefix + "------------ Finished upserting extraction pipeline runs. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "------------ Start deleting extraction pipelines. ------------------");
        List<Item> deleteItemsInput = listPipelinesResults.stream()
                .map(pipeline -> Item.newBuilder()
                        .setExternalId(pipeline.getExternalId())
                        .build())
                .collect(Collectors.toList());

        List<Item> deleteItemsResults = client.extractionPipelines().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "------------ Finished deleting extraction pipelines. Duration: {} -----------",
                Duration.between(startInstant, Instant.now()));

        assertEquals(upsertPipelinesList.size(), listPipelinesResults.size());
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
    }
}