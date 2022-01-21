package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.ThreeDAssetMapping;
import com.cognite.client.dto.ThreeDNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreeDAssetMappingsParser {

    private static final Logger LOG = LoggerFactory.getLogger(ThreeDAssetMappingsParser.class);

    static final ObjectMapper objectMapper = new ObjectMapper();

    public static ThreeDAssetMapping parseThreeDAssetMapping(String json) throws JsonProcessingException {
        JsonNode root = objectMapper.readTree(json);
        ThreeDAssetMapping.Builder builder = ThreeDAssetMapping.newBuilder();

        if (root.isObject()) {
            extractNodes(builder, root);
        }

        return builder.build();
    }

    private static void extractNodes(ThreeDAssetMapping.Builder builder, JsonNode node) {
        if (node.path("nodeId").isIntegralNumber()) {
            builder.setNodeId(node.get("nodeId").longValue());
        }
        if (node.path("assetId").isIntegralNumber()) {
            builder.setAssetId(node.get("assetId").longValue());
        }
        if (node.path("treeIndex").isIntegralNumber()) {
            builder.setTreeIndex(node.get("treeIndex").longValue());
        }
        if (node.path("subtreeSize").isIntegralNumber()) {
            builder.setSubtreeSize(node.get("subtreeSize").longValue());
        }
    }
}
