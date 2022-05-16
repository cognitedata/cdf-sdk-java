package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.*;
import com.cognite.client.util.DataGenerator;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequencesIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteSequences() throws Exception {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeReadAndDeleteSequences() -";
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

        try {
            LOG.info(loggingPrefix + "Start upserting sequences.");
            List<SequenceMetadata> upsertSequencesList = DataGenerator.generateSequenceMetadata(153);
            client.sequences().upsert(upsertSequencesList);
            LOG.info(loggingPrefix + "Finished upserting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(10000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading sequences.");
            List<SequenceMetadata> listSequencesResults = new ArrayList<>();
            client.sequences()
                    .list(Request.create()
                            .withFilterMetadataParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(sequences -> listSequencesResults.addAll(sequences));
            LOG.info(loggingPrefix + "Finished reading sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting sequences.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listSequencesResults.stream()
                    .map(sequences -> Item.newBuilder()
                            .setExternalId(sequences.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.sequences().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertSequencesList.size(), listSequencesResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteSequencesRows() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDeleteSequencesRows() -";
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

        try {
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");
            LOG.info(loggingPrefix + "Start upserting sequences headers.");
            List<SequenceMetadata> upsertSequencesList = DataGenerator.generateSequenceMetadata(53);
            client.sequences().upsert(upsertSequencesList);
            LOG.info(loggingPrefix + "Finished upserting sequences headers. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            Thread.sleep(1000); // Pause... just in case

            LOG.info(loggingPrefix + "----------------------------------------------------------------------");
            LOG.info(loggingPrefix + "Start upserting sequences rows.");
            List<SequenceBody> upsertSequenceBodyList = new ArrayList<>();
            upsertSequencesList.forEach(sequence ->
                    upsertSequenceBodyList.add(DataGenerator.generateSequenceRows(sequence, 567)));
            List<SequenceBody> upsertSequenceBodyResponse = client.sequences().rows().upsert(upsertSequenceBodyList);
            LOG.info(loggingPrefix + "Finished upserting sequences rows. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            Thread.sleep(10000); // Wait for evt. consistency

            LOG.info(loggingPrefix + "----------------------------------------------------------------------");
            LOG.info(loggingPrefix + "Start reading sequences headers.");
            List<SequenceMetadata> listSequencesResults = new ArrayList<>();
            client.sequences()
                    .list(Request.create()
                            .withFilterMetadataParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(sequences -> listSequencesResults.addAll(sequences));
            LOG.info(loggingPrefix + "Finished reading sequences headers. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "----------------------------------------------------------------------");
            LOG.info(loggingPrefix + "Start reading sequences rows.");
            List<SequenceBody> listSequencesRowsResults = new ArrayList<>();
            List<Item> sequenceBodyRequestItems = listSequencesResults.stream()
                    .map(sequenceMetadata -> Item.newBuilder().setId(sequenceMetadata.getId()).build())
                    .collect(Collectors.toList());
            client.sequences().rows()
                    .retrieveComplete(sequenceBodyRequestItems)
                    .forEachRemaining(sequenceBodies -> listSequencesRowsResults.addAll(sequenceBodies));
            LOG.info(loggingPrefix + "Finished reading sequences rows. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "----------------------------------------------------------------------");
            LOG.info(loggingPrefix + "Start deleting sequences rows.");
            List<SequenceBody> deleteRowsInput = listSequencesRowsResults;
            List<SequenceBody> deleteRowsResults = client.sequences().rows().delete(deleteRowsInput);
            LOG.info(loggingPrefix + "Finished deleting sequences rows. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "----------------------------------------------------------------------");
            LOG.info(loggingPrefix + "Start deleting sequences.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listSequencesResults.stream()
                    .map(sequences -> Item.newBuilder()
                            .setExternalId(sequences.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.sequences().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            assertEquals(upsertSequencesList.size(), listSequencesResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeEditReadAndDeleteSequencesRows() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeEditReadAndDeleteSequencesRows() -";
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

        try {
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");
            LOG.info(loggingPrefix + "Start upserting sequences headers.");
            List<SequenceMetadata> upsertSequencesList = DataGenerator.generateSequenceMetadata(11);
            client.sequences().upsert(upsertSequencesList);
            LOG.info(loggingPrefix + "Finished upserting sequences headers. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            Thread.sleep(1000); // Pause... just in case

            LOG.info(loggingPrefix + "----------------------------------------------------------------------");
            LOG.info(loggingPrefix + "Start upserting sequences rows.");
            List<SequenceBody> upsertSequenceBodyList = new ArrayList<>();
            upsertSequencesList.forEach(sequence ->
                    upsertSequenceBodyList.add(DataGenerator.generateSequenceRows(sequence, 56)));
            List<SequenceBody> upsertSequenceBodyResponse = client.sequences().rows().upsert(upsertSequenceBodyList);
            LOG.info(loggingPrefix + "Finished upserting sequences rows. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            Thread.sleep(5000); // Wait for evt. consistency

            LOG.info(loggingPrefix + "----------------------------------------------------------------------");
            LOG.info(loggingPrefix + "Start reading sequences headers.");
            List<SequenceMetadata> listSequencesResults = new ArrayList<>();
            client.sequences()
                    .list(Request.create()
                            .withFilterMetadataParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(sequences -> listSequencesResults.addAll(sequences));

            BooleanSupplier verifySequenceHeaders = () -> {
                Map<String, SequenceMetadata> upsertMap = upsertSequencesList.stream()
                        .collect(Collectors.toMap(SequenceMetadata::getExternalId, Function.identity()));
                if (upsertMap.size() != listSequencesResults.size()) {
                    return false;
                }
                for (SequenceMetadata sequenceMetadata: listSequencesResults) {
                    SequenceMetadata original = upsertMap.getOrDefault(sequenceMetadata.getExternalId(), SequenceMetadata.getDefaultInstance());
                    if (!(sequenceMetadata.getName().equals(original.getName())
                            && sequenceMetadata.getDescription().equals(original.getDescription())
                            && sequenceMetadata.getMetadataCount() == original.getMetadataCount()
                            && sequenceMetadata.getColumnsCount() == original.getColumnsCount()
                            && sequenceMetadata.getAssetId() == original.getAssetId())) {
                        return false;
                    }
                }

                return true;
            };

            assertTrue(verifySequenceHeaders, "Sequence header upsert not correct");

            LOG.info(loggingPrefix + "Finished reading sequences headers. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "----------------------------------------------------------------------");
            LOG.info(loggingPrefix + "Start reading sequences rows.");
            List<SequenceBody> listSequencesRowsResults = new ArrayList<>();
            List<Item> sequenceBodyRequestItems = listSequencesResults.stream()
                    .map(sequenceMetadata -> Item.newBuilder().setId(sequenceMetadata.getId()).build())
                    .collect(Collectors.toList());
            client.sequences().rows()
                    .retrieveComplete(sequenceBodyRequestItems)
                    .forEachRemaining(sequenceBodies -> listSequencesRowsResults.addAll(sequenceBodies));

            BooleanSupplier verifySequenceRowOriginal = () -> isEqual(upsertSequenceBodyList, listSequencesRowsResults);
            assertTrue(verifySequenceRowOriginal, "Sequence row upsert not correct");

            LOG.info(loggingPrefix + "Finished reading sequences rows. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "----------------------------------------------------------------------");
            LOG.info(loggingPrefix + "Start updating sequences rows with new columns.");
            List<SequenceBody> updatedSequenceBodyList = new ArrayList<>();
            for (SequenceBody originalBody : upsertSequenceBodyList) {
                List<SequenceColumn> newColumns = DataGenerator.generateSequenceColumnHeader(2);

                // Add row values for the new columns
                List<SequenceRow> modifiedRowList = new ArrayList<>();
                for (SequenceRow row : originalBody.getRowsList()) {
                    SequenceRow.Builder rowBuilder = row.toBuilder();
                    for (SequenceColumn column : newColumns) {
                        if (column.getValueType() == SequenceColumn.ValueType.DOUBLE) {
                            rowBuilder.addValues(Values.of(ThreadLocalRandom.current().nextDouble(1000000d)));
                        } else if (column.getValueType() == SequenceColumn.ValueType.LONG) {
                            rowBuilder.addValues(Values.of(ThreadLocalRandom.current().nextLong(10000000)));
                        } else {
                            rowBuilder.addValues(Values.of(RandomStringUtils.randomAlphanumeric(5, 30)));
                        }
                    }
                    modifiedRowList.add(rowBuilder.build());
                }

                SequenceBody modified = originalBody.toBuilder()
                        .addAllColumns(newColumns)
                        .clearRows()
                        .addAllRows(modifiedRowList)
                        .build();
                updatedSequenceBodyList.add(modified);
            }

            client.sequences().rows().upsert(updatedSequenceBodyList);

            List<SequenceBody> modifiedSequenceBodyResponse = new ArrayList<>();
            client.sequences().rows()
                    .retrieveComplete(sequenceBodyRequestItems)
                    .forEachRemaining(sequenceBodies -> modifiedSequenceBodyResponse.addAll(sequenceBodies));

            BooleanSupplier verifySequenceRowModified = () -> isEqual(updatedSequenceBodyList, modifiedSequenceBodyResponse);
            assertTrue(verifySequenceRowModified, "Modified sequence row upsert not correct");

            LOG.info(loggingPrefix + "Finished updating sequences rows with new columns. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            Thread.sleep(5000); // Wait for evt. consistency

            LOG.info(loggingPrefix + "----------------------------------------------------------------------");
            LOG.info(loggingPrefix + "Start deleting sequences rows.");
            List<SequenceBody> deleteRowsInput = listSequencesRowsResults;
            List<SequenceBody> deleteRowsResults = client.sequences().rows().delete(deleteRowsInput);
            LOG.info(loggingPrefix + "Finished deleting sequences rows. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "----------------------------------------------------------------------");
            LOG.info(loggingPrefix + "Start deleting sequences.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listSequencesResults.stream()
                    .map(sequences -> Item.newBuilder()
                            .setExternalId(sequences.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.sequences().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            assertEquals(upsertSequencesList.size(), listSequencesResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            Thread.sleep(1000);
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeEditAndDeleteSequences() throws Exception {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeEditAndDeleteSequences() -";
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

        try {
            LOG.info(loggingPrefix + "Start upserting sequences.");
            List<SequenceMetadata> upsertSequencesList = DataGenerator.generateSequenceMetadata(12);
            List<SequenceMetadata> upsertedTimeseries = client.sequences().upsert(upsertSequencesList);
            LOG.info(loggingPrefix + "Finished upserting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(3000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start updating sequences.");
            List<SequenceMetadata> editedSequencesInput = upsertedTimeseries.stream()
                    .map(sequences -> sequences.toBuilder()
                                .setDescription("new-value")
                                .clearMetadata()
                                .putMetadata("new-key", "new-value")
                                .addAllColumns(DataGenerator.generateSequenceColumnHeader(2))
                                .removeColumns(1)
                                .build())
                    .map(sequence -> {
                        // modify all columns
                        List<SequenceColumn> modifiedColumns = sequence.getColumnsList().stream()
                                .map(column -> column.toBuilder()
                                        .clearMetadata()
                                        .putMetadata("new-column-key", "new-column-value")
                                        .build())
                                .collect(Collectors.toList());

                        return sequence.toBuilder()
                                .clearColumns()
                                .addAllColumns(modifiedColumns)
                                .build();
                    })
                    .collect(Collectors.toList());

            List<SequenceMetadata> sequencesUpdateResults = client.sequences().upsert(editedSequencesInput);
            LOG.info(loggingPrefix + "Finished updating sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start update replace sequences.");
            client = client
                    .withClientConfig(ClientConfig.create()
                            .withUpsertMode(UpsertMode.REPLACE));

            List<SequenceMetadata> sequencesReplaceResults = client.sequences().upsert(editedSequencesInput);
            LOG.info(loggingPrefix + "Finished update replace sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(3000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading sequences.");
            List<SequenceMetadata> listSequencesResults = new ArrayList<>();
            client.sequences()
                    .list(Request.create()
                            .withFilterMetadataParameter("new-key", "new-value"))
                    .forEachRemaining(sequences -> listSequencesResults.addAll(sequences));
            LOG.info(loggingPrefix + "Finished reading sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting sequences.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listSequencesResults.stream()
                    .map(sequences -> Item.newBuilder()
                            .setExternalId(sequences.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.sequences().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            BooleanSupplier updateCondition = () -> {
                for (SequenceMetadata sequences : sequencesUpdateResults)  {
                    if (sequences.getDescription().equals("new-value")
                            && sequences.containsMetadata("new-key")
                            && sequences.containsMetadata(DataGenerator.sourceKey)
                            && sequences.getColumnsList().stream().anyMatch(sequenceColumn -> sequenceColumn.containsMetadata(DataGenerator.sourceKey))
                            && sequences.getColumnsList().stream().anyMatch(sequenceColumn -> sequenceColumn.containsMetadata("new-column-key"))
                    ) {
                        // all good
                    } else {
                        return false;
                    }
                }
                return true;
            };

            BooleanSupplier replaceCondition = () -> {
                for (SequenceMetadata sequences : sequencesReplaceResults)  {
                    if (sequences.getDescription().equals("new-value")
                            && sequences.containsMetadata("new-key")
                            && !sequences.containsMetadata(DataGenerator.sourceKey)
                            && sequences.getColumnsList().stream().allMatch(sequenceColumn -> !sequenceColumn.containsMetadata(DataGenerator.sourceKey))
                            && sequences.getColumnsList().stream().allMatch(sequenceColumn -> sequenceColumn.containsMetadata("new-column-key"))
                    ) {
                        // all good
                    } else {
                        return false;
                    }
                }
                return true;
            };

            assertTrue(updateCondition, "Sequences update not correct");
            assertTrue(replaceCondition, "Sequences replace not correct");

            assertEquals(upsertSequencesList.size(), listSequencesResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeRetrieveAndDeleteSequences() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDeleteSequences() -";
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

        try {
            LOG.info(loggingPrefix + "Start upserting sequences.");
            List<SequenceMetadata> upsertSequencesList = DataGenerator.generateSequenceMetadata(168);
            client.sequences().upsert(upsertSequencesList);
            LOG.info(loggingPrefix + "Finished upserting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start listing sequences.");
            List<SequenceMetadata> listSequencesResults = new ArrayList<>();
            client.sequences()
                    .list(Request.create()
                            .withFilterMetadataParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(sequences -> listSequencesResults.addAll(sequences));
            LOG.info(loggingPrefix + "Finished listing sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start retrieving sequences.");
            List<Item> sequencesItems = new ArrayList<>();
            listSequencesResults.stream()
                    .map(aequences -> Item.newBuilder()
                            .setExternalId(aequences.getExternalId())
                            .build())
                    .forEach(item -> sequencesItems.add(item));

            List<SequenceMetadata> retrievedSequences = client.sequences().retrieve(sequencesItems);
            LOG.info(loggingPrefix + "Finished retrieving sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting sequences.");
            List<Item> deleteItemsInput = new ArrayList<>();
            retrievedSequences.stream()
                    .map(sequences -> Item.newBuilder()
                            .setExternalId(sequences.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.sequences().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertSequencesList.size(), listSequencesResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
            assertEquals(sequencesItems.size(), retrievedSequences.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeAggregateAndDeleteSequences() throws Exception {
        int noItems = 145;
        Instant startInstant = Instant.now();

        String loggingPrefix = "UnitTest - writeAggregateAndDeleteSequences() -";
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
            LOG.info(loggingPrefix + "Start upserting sequences.");
            List<SequenceMetadata> upsertSequencesList = DataGenerator.generateSequenceMetadata(noItems);
            client.sequences().upsert(upsertSequencesList);
            LOG.info(loggingPrefix + "Finished upserting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(10000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start aggregating sequences.");
            Aggregate aggregateResult = client.sequences()
                    .aggregate(Request.create()
                            .withFilterMetadataParameter("source", DataGenerator.sourceValue));
            LOG.info(loggingPrefix + "Aggregate results: {}", aggregateResult);
            LOG.info(loggingPrefix + "Finished aggregating sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start reading sequences.");
            List<SequenceMetadata> listSequencesResults = new ArrayList<>();
            client.sequences()
                    .list(Request.create()
                            .withFilterMetadataParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(sequences -> listSequencesResults.addAll(sequences));
            LOG.info(loggingPrefix + "Finished reading sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting sequences.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listSequencesResults.stream()
                    .map(sequences -> Item.newBuilder()
                            .setExternalId(sequences.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.sequences().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertSequencesList.size(), listSequencesResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    private boolean isEqual(List<SequenceBody> left, List<SequenceBody> right) {
        String loggingPrefix = "isEqual() - ";
        Map<String, SequenceBody> leftMap = left.stream()
                .collect(Collectors.toMap(SequenceBody::getExternalId, Function.identity()));
        if (leftMap.size() != right.size()) {
            LOG.warn(loggingPrefix + "SequenceBody list inputs not of equal size. Left: {}, right: {}",
                    left.size(),
                    right.size());
            return false;
        }
        for (SequenceBody rightBody: right) {
            // Check the basic content counts.
            SequenceBody leftBody = leftMap.getOrDefault(rightBody.getExternalId(), SequenceBody.getDefaultInstance());
            if (!(rightBody.getColumnsCount() == leftBody.getColumnsCount()
                    && rightBody.getRowsCount() == leftBody.getRowsCount())) {
                LOG.warn(loggingPrefix + "SequenceBody column and row count not of equal size. Left column: {}, "
                        + "right column: {}, left row: {}, right row: {}",
                        leftBody.getColumnsCount(),
                        rightBody.getColumnsCount(),
                        leftBody.getRowsCount(),
                        rightBody.getRowsCount());
                return false;
            }

            // Check the column schema.
            List<SequenceColumn> rightColumns = rightBody.getColumnsList();
            Map<String, SequenceColumn> leftColumns = leftBody.getColumnsList().stream()
                    .collect(Collectors.toMap(SequenceColumn::getExternalId, Function.identity()));
            for (SequenceColumn rightColumn : rightColumns) {
                if (!leftColumns.containsKey(rightColumn.getExternalId())) {
                    LOG.warn(loggingPrefix + "SequenceBody column schema not equal. Left side does not have column extId: {}" + System.lineSeparator()
                                    + "right column: {}",
                            rightColumn.getExternalId(),
                            rightColumn);
                    return false;
                }
            }

            // Check the row values.
            List<SequenceRow> rightRows = rightBody.getRowsList();
            Map<Long, SequenceRow> leftRows = leftBody.getRowsList().stream()
                    .collect(Collectors.toMap(SequenceRow::getRowNumber, Function.identity()));
            for (SequenceRow row : rightRows) {
                if (!(leftRows.containsKey(row.getRowNumber())
                        && leftRows.get(row.getRowNumber()).getValuesCount() == row.getValuesCount())) {
                    LOG.warn(loggingPrefix + "SequenceBody rows counts not equal for row no {}. Left value count: {}, "
                                    + "right value count: {}",
                            row.getRowNumber(),
                            leftRows.get(row.getRowNumber()).getValuesCount(),
                            row.getValuesCount());
                    return false;
                }
                for (int i = 0; i < row.getValuesCount(); i++) {
                    if (!row.getValues(i).equals(leftRows.get(row.getRowNumber()).getValues(i))) {
                        LOG.warn(loggingPrefix + "SequenceBody column value not equal for index {}. Left column: {}" + System.lineSeparator()
                                        + "right column: {}",
                                i,
                                leftRows.get(row.getRowNumber()).getValues(i),
                                row.getValues(i));
                        return false;
                    }
                }
            }
        }

        return true;
    }
}