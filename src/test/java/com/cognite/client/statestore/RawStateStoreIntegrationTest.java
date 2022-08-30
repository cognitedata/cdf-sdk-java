package com.cognite.client.statestore;

import com.cognite.client.CogniteClient;
import com.cognite.client.TestConfigProvider;
import com.cognite.client.config.TokenUrl;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class RawStateStoreIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    void createCommitAndLoadStates() throws Exception {
        final String dbName = "states";
        final String tableName = "stateTable";
        final String lowWatermarkKey = "low";
        final String highWatermarkKey = "high";
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - createCommitAndLoadStates() -";
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
        LOG.info(loggingPrefix + "Creating raw state store and states.");

        StateStore stateStore = RawStateStore.of(client, dbName, tableName)
                .withMaxCommitInterval(Duration.ofSeconds(2));
        stateStore.load(); // should not do anything
        Map<String, Map<String, Long>> statesMap = new HashMap<>();
        for (int i = 0; i < 10000; i++) {
            String key = RandomStringUtils.randomAlphanumeric(10);
            long lowWatermark = ThreadLocalRandom.current().nextLong(Long.MIN_VALUE, 1_661_608_010_960L); // current ms epoch
            long highWatermark = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);

            statesMap.put(key, Map.of(lowWatermarkKey, lowWatermark, highWatermarkKey, highWatermark));
            stateStore.setLow(key, lowWatermark);
            stateStore.setHigh(key, highWatermark);
        }

        stateStore.commit();

        List<Long> lowWatermarks = statesMap.keySet().stream()
                .map(key -> statesMap.get(key).get(lowWatermarkKey))
                .collect(Collectors.toList());
        List<Long> lowWatermarksStateStore = statesMap.keySet().stream()
                .map(key -> stateStore.getLow(key).getAsLong())
                .collect(Collectors.toList());
        List<Long> highWatermarks = statesMap.keySet().stream()
                .map(key -> statesMap.get(key).get(highWatermarkKey))
                .collect(Collectors.toList());
        List<Long> highWatermarksStateStore = statesMap.keySet().stream()
                .map(key -> stateStore.getHigh(key).getAsLong())
                .collect(Collectors.toList());
        assertAll("State store correctness",
                () -> assertIterableEquals(lowWatermarks, lowWatermarksStateStore, "low watermark not equal"),
                () -> assertIterableEquals(highWatermarks, highWatermarksStateStore, "high watermark not equal"));

        LOG.info(loggingPrefix + "Finished creating raw state store and states. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "Load existing local states.");
        stateStore.load();

        List<Long> lowWatermarksLoadStateStore = statesMap.keySet().stream()
                .map(key -> stateStore.getLow(key).getAsLong())
                .collect(Collectors.toList());
        List<Long> highWatermarksLoadStateStore = statesMap.keySet().stream()
                .map(key -> stateStore.getHigh(key).getAsLong())
                .collect(Collectors.toList());
        assertAll("State store correctness",
                () -> assertIterableEquals(lowWatermarks, lowWatermarksLoadStateStore, "low watermark not equal"),
                () -> assertIterableEquals(highWatermarks, highWatermarksLoadStateStore, "high watermark not equal"));

        LOG.info(loggingPrefix + "Finished loading existing local states. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "Start simulating state processing.");
        stateStore.start();
        Instant start = Instant.now();
        while (Duration.between(start, Instant.now()).compareTo(Duration.ofSeconds(10)) < 0) {
            List<String> removeList = statesMap.keySet().stream()
                    .filter(key -> ThreadLocalRandom.current().nextFloat() < 0.02)
                    .collect(Collectors.toList());
            removeList.forEach(key -> {
                stateStore.deleteState(key);
                statesMap.remove(key);
            });

            List<String> keyList = statesMap.keySet().stream()
                    .filter(key -> ThreadLocalRandom.current().nextBoolean())
                    .collect(Collectors.toList());
            for (String key : keyList) {
                long lowWatermark = ThreadLocalRandom.current().nextLong(Long.MIN_VALUE, 1_661_608_010_960L); // current ms epoch
                long highWatermark = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
                stateStore.expandHigh(key, highWatermark);
                stateStore.expandLow(key, lowWatermark);

                long storedHigh = statesMap.get(key).get(highWatermarkKey);
                long storedLow = statesMap.get(key).get(lowWatermarkKey);

                long newHigh = highWatermark > storedHigh ? highWatermark : storedHigh;
                long newLow = lowWatermark < storedLow ? lowWatermark : storedLow;
                statesMap.put(key, Map.of(lowWatermarkKey, newLow, highWatermarkKey, newHigh));
            }
            Thread.sleep(876);
        }
        stateStore.stop();
        client.raw().databases().delete(List.of(dbName), true);

        List<Long> lowWatermarksUpdated = statesMap.keySet().stream()
                .map(key -> statesMap.get(key).get(lowWatermarkKey))
                .collect(Collectors.toList());
        List<Long> highWatermarksUpdated = statesMap.keySet().stream()
                .map(key -> statesMap.get(key).get(highWatermarkKey))
                .collect(Collectors.toList());
        List<Long> lowWatermarksProcessedStateStore = statesMap.keySet().stream()
                .map(key -> stateStore.getLow(key).getAsLong())
                .collect(Collectors.toList());
        List<Long> highWatermarksProcessedStateStore = statesMap.keySet().stream()
                .map(key -> stateStore.getHigh(key).getAsLong())
                .collect(Collectors.toList());
        assertAll("State store correctness",
                () -> assertIterableEquals(lowWatermarksUpdated, lowWatermarksProcessedStateStore, "low watermark not equal"),
                () -> assertIterableEquals(highWatermarksUpdated, highWatermarksProcessedStateStore, "high watermark not equal"),
                () -> assertEquals(statesMap.size(), stateStore.keySet().size(), "Different number of state keys")
        );

        assertAll("Null state handling",
                () -> assertTrue(stateStore.getState("not-valid-key").isEmpty(), "getState not returning empty"),
                () -> assertTrue(stateStore.getHigh("not-valid-key").isEmpty(), "getHigh not returning empty"),
                () -> assertTrue(stateStore.getLow("not-valid-key").isEmpty(), "getLow not returning empty")
        );


        LOG.info(loggingPrefix + "Finished simulating state processing. Duration : {}",
                Duration.between(startInstant, Instant.now()));

    }
}