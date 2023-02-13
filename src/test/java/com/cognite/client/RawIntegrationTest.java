package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.RawRow;
import com.cognite.client.stream.RawPublisher;
import com.cognite.client.util.DataGenerator;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RawIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteRaw() throws Exception {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeReadAndDeleteRaw() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofClientCredentials(
                    TestConfigProvider.getProject(),
                    TestConfigProvider.getClientId(),
                    TestConfigProvider.getClientSecret(),
                    TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                    .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        try {
            LOG.info(loggingPrefix + "Start creating raw databases.");
            int noDatabases = 3;
            List<String> createDatabasesList = DataGenerator.generateListString(noDatabases);
            client.raw().databases().create(createDatabasesList);
            LOG.info(loggingPrefix + "Finished creating raw databases. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start creating raw tables.");
            Map<String, List<String>> createTablesLists = new HashMap<>();
            int noTables = 10;
            for (String dbName : createDatabasesList) {
                createTablesLists.put(dbName, DataGenerator.generateListString(noTables));
                client.raw().tables().create(dbName, createTablesLists.get(dbName), false);
            }
            LOG.info(loggingPrefix + "Finished creating raw tables. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start creating raw rows.");
            String rowDbName = createDatabasesList.get(0);
            String rowTableName = createTablesLists.get(rowDbName).get(0);
            List<RawRow> createRowsList = DataGenerator.generateRawRows(rowDbName, rowTableName, 32983);
            List<RawRow> createRowsResults = client.raw().rows().upsert(createRowsList, false);
            LOG.info(loggingPrefix + "Finished creating raw rows. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start reading raw databases.");
            List<String> listDatabaseResults = new ArrayList<>();
            client.raw().databases()
                    .list()
                    .forEachRemaining(listDatabaseResults::addAll);
            LOG.info(loggingPrefix + "Finished reading databases. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start reading raw tables.");
            Map<String, List<String>> listTablesResults = new HashMap<>();
            for (String dbName : createDatabasesList) {
                List<String> tablesResults = new ArrayList<>();
                client.raw().tables()
                        .list(dbName)
                        .forEachRemaining(tablesResults::addAll);
                listTablesResults.put(dbName, tablesResults);
            }
            LOG.info(loggingPrefix + "Finished reading databases. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start listing raw rows.");
            List<RawRow> listRowsResults = new ArrayList<>();
            client.raw().rows().list(rowDbName, rowTableName)
                    .forEachRemaining(listRowsResults::addAll);
            LOG.info(loggingPrefix + "Finished listing raw rows. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start retrieving raw rows.");
            List<String> rowsToRetrieve = createRowsList.stream()
                    .filter(row -> ThreadLocalRandom.current().nextBoolean())
                    .limit(20)
                    .map(row -> row.getKey())
                    .collect(Collectors.toList());
            List<RawRow> rowsRetrieved = client.raw().rows().retrieve(rowDbName, rowTableName, rowsToRetrieve);
            LOG.info(loggingPrefix + "Finished retrieving raw rows. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start deleting raw rows.");
            List<RawRow> rowsToDelete = createRowsList.stream()
                    .filter(row -> ThreadLocalRandom.current().nextBoolean())
                    .collect(Collectors.toList());
            List<RawRow> deleteRowResults = client.raw().rows().delete(rowsToDelete);
            LOG.info(loggingPrefix + "Finished deleting raw rows. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start deleting raw tables.");
            Map<String, List<String>> deleteTablesResults = new HashMap<>();
            for (String dbName : createDatabasesList) {
                List<String> deleteItemsInput = new ArrayList<>();
                deleteItemsInput.addAll(createTablesLists.get(dbName));

                deleteTablesResults.put(dbName, client.raw().tables().delete(dbName, deleteItemsInput));
            }
            LOG.info(loggingPrefix + "Finished deleting raw tables. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start deleting raw databases.");
            List<String> deleteItemsInput = new ArrayList<>();
            deleteItemsInput.addAll(createDatabasesList);

            List<String> deleteItemsResults = client.raw().databases().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting raw databases. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            // assertEquals(createDatabasesList.size(), listDatabaseResults.size());
            for (String dbName : createDatabasesList) {
                assertEquals(createTablesLists.get(dbName).size(), listTablesResults.get(dbName).size());
                assertEquals(createTablesLists.get(dbName).size(), deleteTablesResults.get(dbName).size());
            }
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
            assertEquals(createRowsList.size(), listRowsResults.size());
            assertEquals(createRowsList.size(), createRowsResults.size());
            assertEquals(rowsToRetrieve.size(), rowsRetrieved.size());
            assertEquals(rowsToDelete.size(), deleteRowResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeStreamAndDeleteRaw() throws Exception {
        Instant startInstant = Instant.now();
        String dbName = "stream-test-db-" + RandomStringUtils.randomAlphanumeric(3);
        String tableName = "stream-test-table-" + RandomStringUtils.randomAlphanumeric(3);

        String loggingPrefix = "UnitTest - writeStreamAndDeleteRaw() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofClientCredentials(
                    TestConfigProvider.getProject(),
                    TestConfigProvider.getClientId(),
                    TestConfigProvider.getClientSecret(),
                    TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                    .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        try {
            LOG.info(loggingPrefix + "Start creating raw database and table.");
            client.raw().tables().create(dbName, List.of(tableName), true);
            LOG.info(loggingPrefix + "Finished creating raw database and table. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Setup the stream subscriber to the raw table.");
            AtomicInteger receiveRowsCount = new AtomicInteger(0);
            List<RawRow> rowList = new CopyOnWriteArrayList<>();

            RawPublisher publisher = client.raw().rows().stream(dbName, tableName)
                    .withStartTime(Instant.now())
                    .withEndTime(Instant.now().plusSeconds(20))
                    .withPollingInterval(Duration.ofSeconds(2))
                    .withConsumer(batch -> {
                            LOG.info(loggingPrefix + "Received a batch of {} rows.",
                                    batch.size());
                            receiveRowsCount.addAndGet(batch.size());
                            rowList.addAll(batch);
            });

            Future<Boolean> streamer = publisher.start();
            LOG.info(loggingPrefix + "Finished setup the stream subscriber to the raw table. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start creating publish raw rows async job.");
            AtomicInteger publishRowsCount = new AtomicInteger(0);
            CompletableFuture.runAsync(() -> {
               for (int i = 0; i < 6; i++) {
                   int noRows = 10;
                   List<RawRow> createRowsList = DataGenerator.generateRawRows(dbName, tableName, noRows);
                   publishRowsCount.addAndGet(noRows);
                   try {
                       client.raw().rows().upsert(createRowsList, false);
                       Thread.sleep(500L);
                   } catch (Exception e) {
                       throw new RuntimeException(e);
                   }
               }
            });

            LOG.info(loggingPrefix + "Finished creating publish raw rows async job. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Wait for stream to finish.");

            LOG.info(loggingPrefix + "Finished reading stream with result {}. Duration: {}",
                    streamer.get(),
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start deleting raw databases.");

            client.raw().databases().delete(List.of(dbName), true);
            LOG.info(loggingPrefix + "Finished deleting raw databases. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            assertEquals(receiveRowsCount.get(), publishRowsCount.get());
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }
}