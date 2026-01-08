package com.cognite.client;

import com.cognite.client.dto.LimitValue;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the LimitValues API.
 *
 * Note: These tests require a valid CDF project with the Limits API enabled.
 * Set the following environment variables:
 * - TEST_PROJECT: Your CDF project name
 * - TEST_CLIENT_ID: OAuth client ID
 * - TEST_CLIENT_SECRET: OAuth client secret
 * - TEST_TENANT_ID: Azure AD tenant ID
 * - TEST_HOST: CDF API host (e.g., https://api.cognitedata.com)
 */
class LimitValuesIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void listLimitValues() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "IntegrationTest - listLimitValues() - ";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");

        CogniteClient client = TestConfigProvider.getCogniteClient();
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start listing limit values.");
        List<LimitValue> listResults = new ArrayList<>();

        try {
            client.limitValues()
                    .list()
                    .forEachRemaining(listResults::addAll);

            LOG.info(loggingPrefix + "Finished listing limit values. Found {} items. Duration: {}",
                    listResults.size(),
                    Duration.between(startInstant, Instant.now()));

            // Log some sample data if available
            if (!listResults.isEmpty()) {
                LimitValue sample = listResults.get(0);
                LOG.info(loggingPrefix + "Sample limit value - limitId: {}", sample.getLimitId());
            }

        } catch (Exception e) {
            LOG.error(loggingPrefix + "Error listing limit values: {}", e.getMessage());
            throw e;
        }

        // The test passes if no exception is thrown
        // The actual count depends on your project's data
        LOG.info(loggingPrefix + "Test completed successfully.");
    }

    @Test
    @Tag("remoteCDP")
    void listLimitValuesWithFilter() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "IntegrationTest - listLimitValuesWithFilter() - ";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");

        CogniteClient client = TestConfigProvider.getCogniteClient();
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "First, listing limit values to derive a prefix for filtering.");
        List<LimitValue> initialResults = new ArrayList<>();
        client.limitValues()
                .list(Request.create().withRootParameter("limit", 10))
                .forEachRemaining(initialResults::addAll);

        if (initialResults.isEmpty()) {
            LOG.warn(loggingPrefix + "No limit values found in project. Skipping filter test.");
            return;
        }

        String sampleLimitId = initialResults.get(0).getLimitId();
        String prefix = sampleLimitId.contains(".")
                ? sampleLimitId.substring(0, sampleLimitId.indexOf('.') + 1)
                : sampleLimitId.substring(0, Math.min(5, sampleLimitId.length()));
        LOG.info(loggingPrefix + "Using derived prefix: '{}'", prefix);

        LOG.info(loggingPrefix + "Start listing limit values with prefix filter.");
        List<LimitValue> filteredResults = new ArrayList<>();

        try {
            Request filterRequest = Request.create()
                    .withFilterParameter("prefix", Map.of(
                            "property", List.of("limitId"),
                            "value", prefix
                    ));

            client.limitValues()
                    .list(filterRequest)
                    .forEachRemaining(filteredResults::addAll);

            LOG.info(loggingPrefix + "Finished listing filtered limit values. Found {} items. Duration: {}",
                    filteredResults.size(),
                    Duration.between(startInstant, Instant.now()));

            // Check all results match the prefix
            for (LimitValue lv : filteredResults) {
                assertTrue(lv.getLimitId().startsWith(prefix),
                        "LimitId should start with prefix: " + prefix);
            }

        } catch (Exception e) {
            LOG.error(loggingPrefix + "Error listing limit values with filter: {}", e.getMessage());
            throw e;
        }

        LOG.info(loggingPrefix + "Test completed successfully.");
    }

    @Test
    @Tag("remoteCDP")
    void retrieveLimitValueById() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "IntegrationTest - retrieveLimitValueById() - ";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");

        CogniteClient client = TestConfigProvider.getCogniteClient();
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        // First, list to get a valid limitId
        LOG.info(loggingPrefix + "First, listing limit values to get a valid ID.");
        List<LimitValue> listResults = new ArrayList<>();

        try {
            client.limitValues()
                    .list(Request.create().withRootParameter("limit", 1))
                    .forEachRemaining(listResults::addAll);

            if (listResults.isEmpty()) {
                LOG.warn(loggingPrefix + "No limit values found in project. Skipping retrieve test.");
                return;
            }

            String limitId = listResults.get(0).getLimitId();
            LOG.info(loggingPrefix + "Found limit value with ID: {}. Now retrieving by ID.", limitId);

            // Retrieve by ID
            LimitValue retrieved = client.limitValues().retrieve(limitId);

            assertNotNull(retrieved, "Retrieved limit value should not be null");
            assertEquals(limitId, retrieved.getLimitId(), "Limit IDs should match");

            LOG.info(loggingPrefix + "Successfully retrieved limit value. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

        } catch (Exception e) {
            LOG.error(loggingPrefix + "Error in test: {}", e.getMessage());
            throw e;
        }

        LOG.info(loggingPrefix + "Test completed successfully.");
    }

    @Test
    @Tag("remoteCDP")
    void retrieveNonExistentLimitValueThrowsException() throws Exception {
        String loggingPrefix = "IntegrationTest - retrieveNonExistentLimitValueThrowsException() - ";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");

        CogniteClient client = TestConfigProvider.getCogniteClient();

        LOG.info(loggingPrefix + "Attempting to retrieve non-existent limit value.");

        assertThrows(Exception.class, () -> {
            client.limitValues().retrieve("non-existent-limit-id-12345");
        }, "Should throw exception for non-existent limit ID");

        LOG.info(loggingPrefix + "Test completed successfully - exception was thrown as expected.");
    }
}

