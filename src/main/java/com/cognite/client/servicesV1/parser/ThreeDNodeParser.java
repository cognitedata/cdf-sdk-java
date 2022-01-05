package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.ThreeDAvailableOutput;
import com.cognite.client.dto.ThreeDNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ThreeDNodeParser {

    private static final Logger LOG = LoggerFactory.getLogger(ThreeDNodeParser.class);

    static final ObjectMapper objectMapper = new ObjectMapper();

    public static List<ThreeDNode> parseThreeDNodes(String json) throws JsonProcessingException {
        List<ThreeDNode> list = new ArrayList<>();
        JsonNode root = objectMapper.readTree(json);
        ThreeDNode.Builder builder = ThreeDNode.newBuilder();

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

    private static void extractNodes(ThreeDNode.Builder builder, JsonNode node) {
        if (node.path("id").isIntegralNumber()) {
            builder.setId(node.get("id").longValue());
        }
        if (node.path("treeIndex").isIntegralNumber()) {
            builder.setTreeIndex(node.get("treeIndex").longValue());
        }
        if (node.path("parentId").isIntegralNumber()) {
            builder.setParentId(node.get("parentId").longValue());
        }
        if (node.path("depth").isIntegralNumber()) {
            builder.setDepth(node.get("depth").longValue());
        }
        if (node.path("name").isTextual()) {
            builder.setName(node.get("name").textValue());
        }
        if (node.path("subtreeSize").isIntegralNumber()) {
            builder.setSubtreeSize(node.get("subtreeSize").longValue());
        }

        if (node.path("boundingBox").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = node.path("boundingBox").fields();
            ThreeDNode.BoundingBox.Builder builderBound = ThreeDNode.BoundingBox.newBuilder();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                if (entry.getKey().equals("max")) {
                    entry.getValue().forEach(value-> builderBound.addMax(value.doubleValue()));
                } else if (entry.getKey().equals("min")) {
                    entry.getValue().forEach(value-> builderBound.addMin(value.doubleValue()));
                } else {
                    LOG.warn("BoundingBox PROPERTY {} IS NOT MAPPED IN OBJECT ThreeDNode ", entry.getKey());
                }
            }
            builder.setBoundingBox(builderBound.build());
        }

        if (node.path("properties").isObject()) {
            ThreeDNode.Properties.Builder builderProps = ThreeDNode.Properties.newBuilder();
            Iterator<Map.Entry<String, JsonNode>> it = node.path("properties").fields();

            while (it.hasNext()) {
                ThreeDNode.Categories.Builder builderCat = ThreeDNode.Categories.newBuilder();
                Map.Entry<String, JsonNode> entry = it.next();
                JsonNode jsonNode = entry.getValue();
                ThreeDNode.Categories.CategoriesValues.Builder builderCatValues = ThreeDNode.Categories.CategoriesValues.newBuilder();
                jsonNode.forEach(js -> {
                    builderCatValues.putCategoriesValues(js.get(0).textValue(), js.get(1).textValue());
                });
                builderCat.putValues(entry.getKey(), builderCatValues.build());
                builderProps.addCategories(builderCat.build());
            }
            builder.setProperties(builderProps.build());
        }
    }
}
