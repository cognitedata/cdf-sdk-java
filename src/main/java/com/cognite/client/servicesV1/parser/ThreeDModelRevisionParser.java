package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.ThreeDModelRevision;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ThreeDModelRevisionParser {

    private static final Logger LOG = LoggerFactory.getLogger(ThreeDModelRevisionParser.class);

    static final ObjectMapper objectMapper = new ObjectMapper();

    public static ThreeDModelRevision parseThreeDModelRevision(String json) throws JsonProcessingException {
        JsonNode root = objectMapper.readTree(json);
        ThreeDModelRevision.Builder tmBuilder = ThreeDModelRevision.newBuilder();

        if (root.path("items").isArray()) {
            for (JsonNode node : root.path("items")) {
                extractNodes(tmBuilder, node);
            }
        }else if (root.isObject()) {
            extractNodes(tmBuilder, root);
        }

        return tmBuilder.build();
    }

    public static List<ThreeDModelRevision> parseThreeDModelToList(String json) throws JsonProcessingException {
        List<ThreeDModelRevision> list = new ArrayList<>();
        JsonNode root = objectMapper.readTree(json);
        ThreeDModelRevision.Builder tmBuilder = ThreeDModelRevision.newBuilder();

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

    private static void extractNodes(ThreeDModelRevision.Builder tmBuilder, JsonNode node) {
        if (node.path("id").isIntegralNumber()) {
            tmBuilder.setId(node.get("id").longValue());
        }
        if (node.path("fileId").isIntegralNumber()) {
            tmBuilder.setFileId(node.get("fileId").longValue());
        }
        if (node.path("status").isTextual()) {
            tmBuilder.setStatus(node.get("status").textValue());
        }
        if (node.path("thumbnailThreedFileId").isIntegralNumber()) {
            tmBuilder.setThumbnailThreedFileId(node.get("thumbnailThreedFileId").longValue());
        }
        if (node.path("thumbnailURL").isTextual()) {
            tmBuilder.setThumbnailURL(node.get("thumbnailURL").textValue());
        }
        if (node.path("assetMappingCount").isIntegralNumber()) {
            tmBuilder.setAssetMappingCount(node.get("assetMappingCount").longValue());
        }
        if (node.path("createdTime").isIntegralNumber()) {
            tmBuilder.setCreatedTime(node.get("createdTime").longValue());
        }
        if (node.path("published").isBoolean()) {
            tmBuilder.setPublished(node.get("published").booleanValue());
        }
        if (node.path("camera").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> tmterator = node.path("camera").fields();
            ThreeDModelRevision.Camera.Builder builderCamera = ThreeDModelRevision.Camera.newBuilder();
            while (tmterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = tmterator.next();
                if (entry.getKey().equals("target")) {
                    entry.getValue().forEach(value-> builderCamera.addTarget(value.doubleValue()));
                } else if (entry.getKey().equals("position")) {
                    entry.getValue().forEach(value-> builderCamera.addPosition(value.doubleValue()));
                } else {
                    LOG.warn("CAMERA PROPERTY {} IS NOT MAPPED IN OBJECT ThreeDModelRevision ", entry.getKey());
                }
            }
            tmBuilder.setCamera(builderCamera.build());
        }
        if (node.path("rotation").isArray()) {
            JsonNode nodeRt = node.path("rotation");
            nodeRt.forEach(value-> tmBuilder.addRotation(value.doubleValue()));
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
     * Builds a request insert item object from {@link ThreeDModelRevision}.
     *
     * An insert item object creates a new 3D Model Revisions object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestInsertItem(ThreeDModelRevision element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        Preconditions.checkNotNull(element.getFileId(), "Unable to find attribute [fileId] in the in the 3d model revisions object. ");
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        // Required fields
        if (element.hasFileId()) {
            mapBuilder.put("fileId", element.getFileId());
        }

        if (element.hasCamera()) {
            mapBuilder.put("camera", element.getCamera());
        }
        if (element.getMetadataCount() > 0) {
            mapBuilder.put("metadata", element.getMetadataMap());
        }
        if (element.getRotationCount() > 0) {
            mapBuilder.put("rotation", element.getRotationList());
        }

        return mapBuilder.build();
    }

    public static Map<String, Object> toRequestUpdateItem(ThreeDModelRevision element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        Preconditions.checkArgument(element.hasId(),
                "Element must have Id in order to be written as an update");
        Preconditions.checkNotNull(element.getFileId(), "Unable to find attribute [fileId] in the in the 3d model revisions object. ");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();

        // Required fields
        mapBuilder.put("id", element.getId());

        if (element.hasPublished()) {
            updateNodeBuilder.put("published", ImmutableMap.of("set", element.getPublished()));
        }
        if (element.hasCamera()) {
            updateNodeBuilder.put("camera", ImmutableMap.of("set", element.getCamera()));
        }
        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("add", element.getMetadataMap()));
        }
        if (element.getRotationCount() > 0) {
            updateNodeBuilder.put("rotation", ImmutableMap.of("set", element.getRotationList()));
        }
        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    public static Map<String, Object> toRequestReplaceItem(ThreeDModelRevision element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        Preconditions.checkArgument(element.hasId(),
                "Element must have Id in order to be written as an update");
        Preconditions.checkNotNull(element.getFileId(), "Unable to find attribute [fileId] in the in the 3d model revisions object. ");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();

        // Required fields
        mapBuilder.put("id", element.getId());

        if (element.hasPublished()) {
            updateNodeBuilder.put("published", ImmutableMap.of("set", element.getPublished()));
        }

        if (element.hasCamera()) {
            updateNodeBuilder.put("camera", ImmutableMap.of("set", element.getCamera()));
        } else {
            updateNodeBuilder.put("camera", ImmutableMap.of("set", ImmutableMap.<String, String>of()));
        }

        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", element.getMetadataMap()));
        } else {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", ImmutableMap.<String, String>of()));
        }

        if (element.getRotationCount() > 0) {
            updateNodeBuilder.put("rotation", ImmutableMap.of("set", element.getRotationList()));
        } else {
            updateNodeBuilder.put("rotation", ImmutableMap.of("set", ImmutableList.of()));
        }
        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }
}


