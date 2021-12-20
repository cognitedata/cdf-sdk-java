package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.ExtractionPipeline;
import com.cognite.client.dto.FileMetadata;
import com.cognite.client.dto.ThreeDModel;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import okhttp3.MediaType;
import okhttp3.RequestBody;

import java.util.*;


/**
 * This class contains a set of methods to help parsing file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class ThreeDModelParser {

    static final String logPrefix = "AssetParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses an 3D Models json string to <code>ThreeDModel</code> proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static ThreeDModel parseThreeDModel(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        ThreeDModel.Builder tmBuilder = ThreeDModel.newBuilder();

        if (root.path("items").isArray()) {
            for (JsonNode node : root.path("items")) {
                extractNodes(tmBuilder, node);
            }
        }else if (root.isObject()) {
            extractNodes(tmBuilder, root);
        }

        return tmBuilder.build();
    }

    /**
     * Parses an 3D Models json string to List <code>ThreeDModel</code> proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static List<ThreeDModel> parseThreeDModelToList(String json) throws Exception {
        List<ThreeDModel> list = new ArrayList<>();
        JsonNode root = objectMapper.readTree(json);
        ThreeDModel.Builder tmBuilder = ThreeDModel.newBuilder();

        if (root.path("items").isArray()) {
            for (JsonNode node : root.path("items")) {
                extractNodes(tmBuilder, node);
                list.add(tmBuilder.build());
            }
        }else if (root.isObject()) {
            extractNodes(tmBuilder, root);
            list.add(tmBuilder.build());
        }

        return list;
    }

    private static void extractNodes(ThreeDModel.Builder tmBuilder, JsonNode node) {
        if (node.path("id").isIntegralNumber()) {
            tmBuilder.setId(node.get("id").longValue());
        }
        if (node.path("name").isTextual()) {
            tmBuilder.setName(node.get("name").textValue());
        }
        if (node.path("createdTime").isIntegralNumber()) {
            tmBuilder.setCreatedTime(node.get("createdTime").longValue());
        }
        if (node.path("dataSetId").isIntegralNumber()) {
            tmBuilder.setDataSetId(node.get("dataSetId").longValue());
        }
        if (node.path("metadata").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> tmterator =
                    node.path("metadata").fields();
            while (tmterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = tmterator.next();
                if (entry.getValue().isTextual()) {
                    tmBuilder.putMetadata(entry.getKey(), entry.getValue().textValue());
                }
            }
        }
    }

    /**
     * Builds a request insert item object from {@link ThreeDModel}.
     *
     * An insert item object creates a new 3D Models object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestInsertItem(ThreeDModel element) throws Exception {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        // Required fields
        if (element.hasName()) {
            mapBuilder.put("name", element.getName());
        } else {
            throw new Exception("Unable to find attribute [name] in the in the 3d model object. ");
        }

        // Optional fields
        if (element.hasDataSetId()) {
            mapBuilder.put("dataSetId", element.getDataSetId());
        }

        if (element.getMetadataCount() > 0) {
            mapBuilder.put("metadata", element.getMetadataMap());
        }

        return mapBuilder.build();
    }

    /**
     * Builds a request update item object from {@link ThreeDModel}.
     *
     * An update item object updates an existing 3D Models object with new values for all provided fields.
     * Fields that are not in the update object retain their original value.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestUpdateItem(ThreeDModel element) throws Exception {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        Preconditions.checkArgument(element.hasId(),
                "Element must have Id in order to be written as an update");
        Preconditions.checkArgument(element.hasName(),
                "Element must have Name in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();

        // Required fields
        mapBuilder.put("id", element.getId());
        updateNodeBuilder.put("name", ImmutableMap.of("set", element.getName()));

        // Optional fields
        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));
        }

        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("add", element.getMetadataMap()));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    /**
     * Builds a request replace item object from {@link ThreeDModel}.
     *
     * A replace item object replaces an existing 3D Models object with new values for all provided fields.
     * Fields that are not in the update object are set to null.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestReplaceItem(ThreeDModel element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        Preconditions.checkArgument(element.hasId(),
                "Element must have Id in order to be written as an update");
        Preconditions.checkArgument(element.hasName(),
                "Element must have Name in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();

        mapBuilder.put("id", element.getId());
        updateNodeBuilder.put("name", ImmutableMap.of("set", element.getName()));

        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));
        } else {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("setNull", true));
        }

        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", element.getMetadataMap()));
        } else {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", ImmutableMap.<String, String>of()));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }
}
