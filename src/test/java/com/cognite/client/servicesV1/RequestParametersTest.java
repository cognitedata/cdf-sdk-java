package com.cognite.client.servicesV1;

import com.cognite.client.Request;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RequestParametersTest {
    final String key = RandomStringUtils.randomAlphanumeric(32);
    final String value = RandomStringUtils.randomAlphanumeric(64);
    final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testJsonInput() throws Exception {
        Request parameters3 = Request.create()
                .withRequestJson(generateJsonString());

        System.out.println("From Json input");
        System.out.println(parameters3.getRequestParametersAsJson());

        assertEquals(generateJsonString(), parameters3.getRequestParametersAsJson());
    }

    @Test
    void getRequestParametersAsJson() throws Exception {
        Request parameters = Request.create()
                .withRequestParameters(generateRequestParameters());

        Request parameters1 = Request.create()
                .withRootParameter("filter", ImmutableMap.<String, Object>of("startTime", 123454321,
                        "description", "descriptionValue",
                        "metadata", ImmutableMap.<String, Object>of("metaA", "valueMetaA",
                                "metaB", "valueMetaB"),
                        "assetIds", ImmutableList.<Long>of(1L, 2L, 3L)))
                .withRootParameter("limit", 1000);

        Request parameters2 = Request.create()
                .withFilterParameter("startTime", 12345432)
                .withFilterParameter("description", "descriptionValue")
                .withFilterParameter("assetIds", ImmutableList.<Long>of(1L, 2L, 3L))
                .withFilterMetadataParameter("metaA", "ValueMetaA")
                .withRootParameter("limit", 100)
                .withFilterMetadataParameter("metaB", "ValueMetaB");

        String firstJson = parameters.getRequestParametersAsJson();
        String secondJson = parameters1.getRequestParametersAsJson();
        String thirdJson = parameters2.getRequestParametersAsJson();

        //System.out.println(firstJson);
        //System.out.println(secondJson);
        //System.out.println(thirdJson);

        assertFalse(firstJson.equals(secondJson));
    }

    @Test
    void getRequestParameters() {
        Request parameters = Request.create()
                .withRequestParameters(generateRequestParameters());

        Request parameters1 = Request.create()
                .withRootParameter("filter", ImmutableMap.<String, Object>of("startTime", 123454321,
                        "metadata", ImmutableMap.<String, Object>of("metaA", "valueMetaA",
                                "metaB", "valueMetaB"),
                        "assetIds", ImmutableList.<Long>of(1L, 2L, 3L)))
                .withRootParameter("limit", 1000);

        Request parameters2 = Request.create()
                .withFilterParameter("startTime", 123454321)
                .withFilterParameter("assetIds", ImmutableList.<Long>of(1L, 2L, 3L))
                .withFilterMetadataParameter("metaA", "ValueMetaA")
                .withRootParameter("limit", 1000)
                .withFilterMetadataParameter("metaB", "ValueMetaB");

        assertEquals(parameters.getFilterParameters().get("startTime"), parameters1.getFilterParameters().get("startTime"));
        assertEquals(parameters.getFilterParameters().get("startTime"), parameters2.getFilterParameters().get("startTime"));
        assertEquals(parameters.getRequestParameters().get("limit"), parameters1.getRequestParameters().get("limit"));
        assertEquals(parameters.getRequestParameters().get("limit"), parameters2.getRequestParameters().get("limit"));
        assertEquals(parameters.getMetadataFilterParameters().get("metaA"), parameters1.getMetadataFilterParameters().get("metaA"));
        assertEquals(parameters.getMetadataFilterParameters().get("metaA"), parameters1.getMetadataFilterParameters().get("metaA"));
    }

    /**
     * Check that protobuf items are correctly parsed into a Json structure. This test a custom parser extension for
     * Jackson.
     */
    @Test
    void parseProtoItems() {
        Struct entityA = Struct.newBuilder()
                .putFields("name", Value.newBuilder().setStringValue("23-DB-9101").build())
                .putFields("fooField", Value.newBuilder().setStringValue("bar").build())
                .build();
        Struct entityB = Struct.newBuilder()
                .putFields("name", Value.newBuilder().setStringValue("23-PC-9101").build())
                .putFields("barField", Value.newBuilder().setStringValue("foo").build())
                .build();
        Struct entityC = Struct.newBuilder()
                .putFields("name", Value.newBuilder().setStringValue("343-Å").build())
                .build();
        ImmutableList<Struct> items = ImmutableList.of(entityA, entityB, entityC);

        Request requestParameters = Request.create()
                .withRootParameter("modelId", 1111L)
                .withRootParameter("matchFrom", items)
                .withRootParameter("numMatches", 4);

        try {
            JsonNode root = mapper.readTree(requestParameters.getRequestParametersAsJson());
            assertEquals(root.path("modelId").longValue(), 1111L);
            assertEquals(root.path("numMatches").intValue(), 4);
            assertEquals(root.path("matchFrom").get(0).path("name").textValue(),
                    entityA.getFieldsOrThrow("name").getStringValue());
            assertEquals(root.path("matchFrom").get(1).path("name").textValue(),
                    entityB.getFieldsOrThrow("name").getStringValue());
            assertEquals(root.path("matchFrom").get(2).path("name").textValue(),
                    entityC.getFieldsOrThrow("name").getStringValue());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ImmutableMap<String, Object> generateRequestParameters() {
        ImmutableMap<String, Object> root = ImmutableMap.<String, Object>builder()
                .put("filter", ImmutableMap.<String, Object>of("startTime", 123454321,
                        "metadata", ImmutableMap.<String, Object>of("metaA", "valueMetaA",
                                "metaB", "valueMetaB"),
                        "assetIds", ImmutableList.<Long>of(1L, 2L, 3L)))
                .put("limit", 1000)
                .build();

        return root;
    }

    private String generateJsonString() throws Exception {
        ObjectNode root = JsonNodeFactory.instance.objectNode();
        ObjectNode filter = root.putObject("filter");
        ObjectNode filterMeta = filter.putObject("metaData");
        ArrayNode assetIds = filter.putArray("assetIds");
        filter.put("startTime", 1234532);
        filterMeta.put("metaA", "valueMetaA");
        assetIds.add(1L);
        assetIds.add(2L);
        root.put("limit", 1000);

        return mapper.writeValueAsString(root);
    }
}