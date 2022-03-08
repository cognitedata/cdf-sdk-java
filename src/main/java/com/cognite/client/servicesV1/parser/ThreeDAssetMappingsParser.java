package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.ThreeDAssetMapping;
import com.cognite.client.dto.ThreeDNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ThreeDAssetMappingsParser {

    private static final Logger LOG = LoggerFactory.getLogger(ThreeDAssetMappingsParser.class);

    static final ObjectMapper objectMapper = new ObjectMapper();

    public static List<ThreeDAssetMapping> parseThreeDAssetMappingToList(String json) throws JsonProcessingException {
        List<ThreeDAssetMapping> list = new ArrayList<>();
        JsonNode root = objectMapper.readTree(json);
        ThreeDAssetMapping.Builder builder = ThreeDAssetMapping.newBuilder();
        if (root.path("items").isArray()) {
            for (JsonNode node : root.path("items")) {
                extractNodes(builder, node);
                list.add(builder.build());
                builder.clear();
            }
        } else if (root.isObject()) {
            extractNodes(builder, root);
            list.add(builder.build());
        }
        return list;
    }

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

    public static Map<String, Object> toRequestInsertItem(ThreeDAssetMapping element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        Preconditions.checkNotNull(element.getNodeId(), "Unable to find attribute [nodeId] in the in the 3d asset mappings object. ");
        Preconditions.checkNotNull(element.getAssetId(), "Unable to find attribute [assetId] in the in the 3d asset mappings object. ");
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        mapBuilder.put("nodeId", element.getNodeId());
        mapBuilder.put("assetId", element.getAssetId());
        return mapBuilder.build();
    }

    public static Map<String, Object> toRequestDeleteItem(ThreeDAssetMapping element) {
        return toRequestInsertItem(element);
    }
}
