package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.ThreeDOutput;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class ThreeDOutputsParser {

    static final ObjectMapper objectMapper = new ObjectMapper();

    public static List<ThreeDOutput> parseThreeDOutputs(String json) throws JsonProcessingException {
        List<ThreeDOutput> list = new ArrayList<>();
        JsonNode root = objectMapper.readTree(json);
        ThreeDOutput.Builder builder = ThreeDOutput.newBuilder();

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

    private static void extractNodes(ThreeDOutput.Builder builder, JsonNode node) {
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
