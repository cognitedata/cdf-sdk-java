package com.cognite.client.servicesV1.parser;

import com.cognite.client.ThreeDAvailableOutputs;
import com.cognite.client.dto.ThreeDAvailableOutput;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class ThreeDAvailableOutputsParser {

    static final ObjectMapper objectMapper = new ObjectMapper();

    public static List<ThreeDAvailableOutput> parseThreeDAvailableOutputs(String json) throws JsonProcessingException {
        List<ThreeDAvailableOutput> list = new ArrayList<>();
        JsonNode root = objectMapper.readTree(json);
        ThreeDAvailableOutput.Builder builder = ThreeDAvailableOutput.newBuilder();

        if (root.path("items").isArray()) {
            for (JsonNode node : root.path("items")) {
                extractNodes(builder, node);
                list.add(builder.build());
                builder.clear();
            }
        }else if (root.isObject()) {
            extractNodes(builder, root);
            list.add(builder.build());
        }
        return list;
    }

    private static void extractNodes(ThreeDAvailableOutput.Builder builder, JsonNode node) {
        if (node.path("format").isTextual()) {
            builder.setFormat(node.get("format").textValue());
        }

        if (node.path("version").isIntegralNumber()) {
            builder.setVersion(node.get("version").intValue());
        }

        if (node.path("blobId").isIntegralNumber()) {
            builder.setBlobId(node.get("blobId").longValue());
        }
    }

}
