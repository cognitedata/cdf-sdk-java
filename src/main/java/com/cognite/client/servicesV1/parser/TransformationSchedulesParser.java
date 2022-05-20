package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.Transformation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class TransformationSchedulesParser {

    static final ObjectMapper objectMapper = new ObjectMapper();

    public static Transformation.Schedule parseTransformationSchedules(String json) throws JsonProcessingException {
        JsonNode root = objectMapper.readTree(json);
        Transformation.Schedule.Builder tmBuilder = Transformation.Schedule.newBuilder();

        if (root.isObject()) {
            extractNodes(tmBuilder, root);
        }

        return tmBuilder.build();
    }

    private static void extractNodes(Transformation.Schedule.Builder tmBuilder, JsonNode root) {
        if (root.path("id").isNumber()) {
            tmBuilder.setId(root.get("id").intValue());
        }
        if (root.path("externalId").isTextual()) {
            tmBuilder.setExternalId(root.get("externalId").textValue());
        }
        if (root.path("createdTime").isLong()) {
            tmBuilder.setCreatedTime(root.get("createdTime").longValue());
        }
        if (root.path("lastUpdatedTime").isLong()) {
            tmBuilder.setLastUpdatedTime(root.get("lastUpdatedTime").longValue());
        }
        if (root.path("interval").isTextual()) {
            tmBuilder.setInterval(root.get("interval").textValue());
        }
        if (root.path("isPaused").isBoolean()) {
            tmBuilder.setIsPaused(root.get("isPaused").booleanValue());
        }
    }

    public static Map<String, Object> toRequestInsertItem(Transformation.Schedule element) throws Exception {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        // Required fields
        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId());
        } else {
            throw new Exception("Unable to find attribute [externalId] in the Transformation object. ");
        }

        if (element.hasInterval()) {
            mapBuilder.put("interval", element.getInterval());
        } else {
            throw new Exception("Unable to find attribute [interval] in the Transformation object. ");
        }

        // Optional fields
        if (element.hasIsPaused()) {
            mapBuilder.put("isPaused", element.getIsPaused());
        }
        return mapBuilder.build();
    }

    public static Map<String, Object> toRequestUpdateItem(Transformation.Schedule element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();

        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId());
        } else {
            mapBuilder.put("id", element.getId());
        }

        if (element.hasInterval()) {
            updateNodeBuilder.put("interval", ImmutableMap.of("set", element.getInterval()));
        }

        if (element.hasIsPaused()) {
            updateNodeBuilder.put("isPaused", ImmutableMap.of("set", element.getIsPaused()));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    public static Map<String, Object> toRequestReplaceItem(Transformation.Schedule element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();

        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId());
        } else {
            mapBuilder.put("id", element.getId());
        }

        if (element.hasInterval()) {
            updateNodeBuilder.put("interval", ImmutableMap.of("set", element.getInterval()));
        }

        if (element.hasIsPaused()) {
            updateNodeBuilder.put("isPaused", ImmutableMap.of("set", element.getIsPaused()));
        } else {
            updateNodeBuilder.put("isPaused", ImmutableMap.of("setNull", true));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }
}
