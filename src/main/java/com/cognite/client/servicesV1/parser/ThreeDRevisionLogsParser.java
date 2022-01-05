package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.ThreeDModelRevision;
import com.cognite.client.dto.ThreeDRevisionLog;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class ThreeDRevisionLogsParser {

    static final ObjectMapper objectMapper = new ObjectMapper();

    public static List<ThreeDRevisionLog> parseThreeDRevisionLogs(String json) throws JsonProcessingException {
        List<ThreeDRevisionLog> list = new ArrayList<>();
        JsonNode root = objectMapper.readTree(json);
        ThreeDRevisionLog.Builder tmBuilder = ThreeDRevisionLog.newBuilder();

        if (root.path("items").isArray()) {
            for (JsonNode node : root.path("items")) {
                extractNodes(tmBuilder, node);
                list.add(tmBuilder.build());
                tmBuilder.clear();
            }
        }else if (root.isObject()) {
            extractNodes(tmBuilder, root);
            list.add(tmBuilder.build());
        }
        return list;
    }

    private static void extractNodes(ThreeDRevisionLog.Builder tmBuilder, JsonNode node) {
        if (node.path("timestamp").isIntegralNumber()) {
            tmBuilder.setTimestamp(node.get("timestamp").longValue());
        }
        if (node.path("severity").isIntegralNumber()) {
            tmBuilder.setSeverity(node.get("severity").intValue());
        }
        if (node.path("type").isTextual()) {
            tmBuilder.setType(node.get("type").textValue());
        }
        if (node.path("info").isTextual()) {
            tmBuilder.setType(node.get("info").textValue());
        }
    }


}
