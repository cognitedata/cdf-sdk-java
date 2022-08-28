package com.cognite.client.statestore;

import com.cognite.client.CogniteClient;
import com.cognite.client.TestConfigProvider;
import com.cognite.client.config.TokenUrl;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class LocalStateStoreIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    void createCommitAndLoadStates() throws Exception {
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
        LOG.info(loggingPrefix + "Creating local state store and states.");
        Path stateStorePath = Paths.get("./stateStore.json");
        StateStore stateStore = LocalStateStore.of(stateStorePath);
        Map<String, Map<String, Long>> statesMap = new HashMap<>();
        for (int i = 0; i < 100; i++) {
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

        LOG.info(loggingPrefix + "Finished creating local state store and states. Duration : {}",
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

    }
}