package com.cognite.client;

import com.cognite.client.dto.Event;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.ResponseBinary;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CdfHttpRequestIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteEventsViaJsonStringRequests() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDeleteEventsViaJsonStringRequests() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");

        CogniteClient client = TestConfigProvider.getCogniteClient();
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start upserting events via http request.");

        String cdfHost = TestConfigProvider.getHost();
        String cdfProject = TestConfigProvider.getProject();
        String cdfApiEndpoint = "events";
        URI requestURI = URI.create(String.format("%s/api/v1/projects/%s/%s",
                cdfHost,
                cdfProject,
                cdfApiEndpoint));
        LOG.info("Sending request to URI: {}", requestURI.toString());

        String postBody = """
                {
                    "items": [
                        {
                            "externalId": "id-number-one",
                            "startTime": 1000000,
                            "endTime": 1000345,
                            "description": "test event",
                            "type": "generated_event",
                            "source": "test-event",
                            "metadata": {
                                "type": "test-event"
                            }
                        },
                        {
                            "externalId": "id-number-two",
                            "startTime": 1000000,
                            "endTime": 1000345,
                            "description": "test event-2",
                            "type": "generated_event",
                            "source": "test-event",
                            "metadata": {
                                "type": "test-event"
                            }
                        }
                    ]
                }
                """;

        ResponseBinary responseBinary = client.experimental().cdfHttpRequest(requestURI)
                .withRequestBody(postBody)
                .post();

        assertTrue(responseBinary.getResponse().isSuccessful(), "post request failed");

        LOG.info(loggingPrefix + "Finished upserting events. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        Thread.sleep(2000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start reading events.");
        List<Event> listEventsResults = new ArrayList<>();
        client.events()
                .list(Request.create()
                        .withFilterParameter("source", "test-event"))
                .forEachRemaining(events -> listEventsResults.addAll(events));
        LOG.info(loggingPrefix + "Finished reading events. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start deleting events.");
        List<Item> deleteItemsInput = listEventsResults.stream()
                .map(event -> Item.newBuilder()
                        .setExternalId(event.getExternalId())
                        .build())
                .collect(Collectors.toList());

        List<Item> deleteItemsResults = client.events().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting events. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        assertEquals(2, listEventsResults.size());
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
    }

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteEventsViaRequestObjectRequests() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDeleteEventsViaRequestObjectRequests() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");

        CogniteClient client = TestConfigProvider.getCogniteClient();
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start upserting events via http request.");

        String cdfHost = TestConfigProvider.getHost();
        String cdfProject = TestConfigProvider.getProject();
        String cdfApiEndpoint = "events";
        URI requestURI = URI.create(String.format("%s/api/v1/projects/%s/%s",
                cdfHost,
                cdfProject,
                cdfApiEndpoint));
        LOG.info("Sending request to URI: {}", requestURI.toString());

        // Build the request Json object body as a Java Map
        Map<String, Object> requestBodyObject = new HashMap<>();

        // Our items array is represented by a Java List
        List<Map<String, Object>> itemsList = new ArrayList<>();
        requestBodyObject.put("items", itemsList);

        // Add event Json objects (represented by Java Map) to the list
        Map<String, Object> eventA = new HashMap<>();
        eventA.put("externalId", "id-number-one");



        Request postEventsRequest = Request.create()
                .

        String postBody = """
                {
                    "items": [
                        {
                            "externalId": "id-number-one",
                            "startTime": 1000000,
                            "endTime": 1000345,
                            "description": "test event",
                            "type": "generated_event",
                            "source": "test-event",
                            "metadata": {
                                "type": "test-event"
                            }
                        },
                        {
                            "externalId": "id-number-two",
                            "startTime": 1000000,
                            "endTime": 1000345,
                            "description": "test event-2",
                            "type": "generated_event",
                            "source": "test-event",
                            "metadata": {
                                "type": "test-event"
                            }
                        }
                    ]
                }
                """;

        ResponseBinary responseBinary = client.experimental().cdfHttpRequest(requestURI)
                .withRequestBody(postBody)
                .post();

        assertTrue(responseBinary.getResponse().isSuccessful(), "post request failed");

        LOG.info(loggingPrefix + "Finished upserting events. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        Thread.sleep(2000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start reading events.");
        List<Event> listEventsResults = new ArrayList<>();
        client.events()
                .list(Request.create()
                        .withFilterParameter("source", "test-event"))
                .forEachRemaining(events -> listEventsResults.addAll(events));
        LOG.info(loggingPrefix + "Finished reading events. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start deleting events.");
        List<Item> deleteItemsInput = listEventsResults.stream()
                .map(event -> Item.newBuilder()
                        .setExternalId(event.getExternalId())
                        .build())
                .collect(Collectors.toList());

        List<Item> deleteItemsResults = client.events().delete(deleteItemsInput);
        LOG.info(loggingPrefix + "Finished deleting events. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        assertEquals(2, listEventsResults.size());
        assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
    }
}