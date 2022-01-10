package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.Label;
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

class LabelsIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteLabels() throws Exception {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeReadAndDeleteLabels() -";
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


        LOG.info(loggingPrefix + "Start upserting labels.");
        List<Label> upsertLabelsList = DataGenerator.generateLabels(58);
        client.labels().upsert(upsertLabelsList);
        LOG.info(loggingPrefix + "Finished upserting labels. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        Thread.sleep(5000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start reading labels.");
        List<Label> listLabelsResults = new ArrayList<>();
        client.labels()
                .list(Request.create()
                        .withFilterParameter("externalIdPrefix", DataGenerator.sourceValue))
                .forEachRemaining(labels -> listLabelsResults.addAll(labels));
        LOG.info(loggingPrefix + "Finished reading labels. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start deleting labels.");
        List<Item> deleteItemsInput = new ArrayList<>();
        listLabelsResults.stream()
                .map(labels -> Item.newBuilder()
                        .setExternalId(labels.getExternalId())
                        .build())
                .forEach(item -> deleteItemsInput.add(item));

        List<Item> deleteItemsResults = client.labels().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting labels. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        assertEquals(upsertLabelsList.size(), listLabelsResults.size());
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());

    }

    @Test
    @Tag("remoteCDP")
    void writeEditAndDeleteLabels() throws Exception {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeEditAndDeleteLabels() -";
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


        LOG.info(loggingPrefix + "Start upserting labels.");
        List<Label> upsertLabelsList = DataGenerator.generateLabels(123);
        List<Label> upsertedLabels = client.labels().upsert(upsertLabelsList);
        LOG.info(loggingPrefix + "Finished upserting labels. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        Thread.sleep(3000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start updating labels.");
        List<Label> editedLabelsInput = upsertedLabels.stream()
                .map(labels -> labels.toBuilder()
                        .setDescription("new-value")
                        .build())
                .collect(Collectors.toList());

        List<Label> labelsUpdateResults = client.labels().upsert(editedLabelsInput);
        LOG.info(loggingPrefix + "Finished updating labels. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start update replace timeseries.");
        client = client
                .withClientConfig(ClientConfig.create()
                        .withUpsertMode(UpsertMode.REPLACE));
        List<Label> labelsReplaceResults = client.labels().upsert(editedLabelsInput);
        LOG.info(loggingPrefix + "Finished update replace labels. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        Thread.sleep(3000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start reading labels.");
        List<Label> listLabelsResults = new ArrayList<>();
        client.labels()
                .list(Request.create()
                        .withFilterParameter("externalIdPrefix", DataGenerator.sourceValue))
                .forEachRemaining(labels -> listLabelsResults.addAll(labels));
        LOG.info(loggingPrefix + "Finished reading labels. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start deleting labels.");
        List<Item> deleteItemsInput = new ArrayList<>();
        listLabelsResults.stream()
                .map(label -> Item.newBuilder()
                        .setExternalId(label.getExternalId())
                        .build())
                .forEach(item -> deleteItemsInput.add(item));

        List<Item> deleteItemsResults = client.labels().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting labels. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        BooleanSupplier updateCondition = () -> {
            for (Label label : labelsUpdateResults) {
                if (label.getDescription().equals("new-value")) {
                    // all good
                } else {
                    return false;
                }
            }
            return true;
        };

        BooleanSupplier replaceCondition = () -> {
            for (Label label : labelsReplaceResults) {
                if (label.getDescription().equals("new-value")) {
                    // all good
                } else {
                    return false;
                }
            }
            return true;
        };

        assertTrue(updateCondition, "Labels update not correct");
        assertTrue(replaceCondition, "Labels replace not correct");

        assertEquals(upsertLabelsList.size(), listLabelsResults.size());
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());

    }

}