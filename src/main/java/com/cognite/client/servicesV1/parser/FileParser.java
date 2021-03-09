/*
 * Copyright (c) 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.FileMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class FileParser {
    static final String logPrefix = "FileParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a file metadata/header json string to <code>FileMetadata</code> proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static FileMetadata parseFileMetadata(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        FileMetadata.Builder fileMetaBuilder = FileMetadata.newBuilder();

        // A file must contain id, name, isUploaded, created and updatedTime.
        if (root.path("id").isIntegralNumber()) {
            fileMetaBuilder.setId(Int64Value.of(root.get("id").longValue()));
        } else {
            throw new Exception(FileParser.buildErrorMessage("id", json));
        }

        if (root.path("name").isTextual()) {
            fileMetaBuilder.setName(StringValue.of(root.get("name").textValue()));
        } else {
            throw new Exception(FileParser.buildErrorMessage("name", json));
        }

        if (root.path("directory").isTextual()) {
            fileMetaBuilder.setDirectory(StringValue.of(root.get("directory").textValue()));
        }

        if (root.path("uploaded").isBoolean()) {
            fileMetaBuilder.setUploaded(root.get("uploaded").booleanValue());
        } else {
            throw new Exception(FileParser.buildErrorMessage("uploaded", json));
        }

        if (root.path("createdTime").isIntegralNumber()) {
            fileMetaBuilder.setCreatedTime(Int64Value.of(root.get("createdTime").longValue()));
        } else {
            throw new Exception(FileParser.buildErrorMessage("createdTime", json));
        }

        if (root.path("lastUpdatedTime").isIntegralNumber()) {
            fileMetaBuilder.setLastUpdatedTime(Int64Value.of(root.get("lastUpdatedTime").longValue()));
        } else {
            throw new Exception(FileParser.buildErrorMessage("lastUpdatedTime", json));
        }

        // The rest of the attributes are optional.
        if (root.path("externalId").isTextual()) {
            fileMetaBuilder.setExternalId(StringValue.of(root.get("externalId").textValue()));
        }
        if (root.path("uploadedTime").isIntegralNumber()) {
            fileMetaBuilder.setUploadedTime(Int64Value.of(root.get("uploadedTime").longValue()));
        }
        if (root.path("source").isTextual()) {
            fileMetaBuilder.setSource(StringValue.of(root.get("source").textValue()));
        }
        if (root.path("mimeType").isTextual()) {
            fileMetaBuilder.setMimeType(StringValue.of(root.get("mimeType").textValue()));
        }
        if (root.path("sourceCreatedTime").isIntegralNumber()) {
            fileMetaBuilder.setSourceCreatedTime(Int64Value.of(root.get("sourceCreatedTime").longValue()));
        }
        if (root.path("sourceModifiedTime").isIntegralNumber()) {
            fileMetaBuilder.setSourceModifiedTime(Int64Value.of(root.get("sourceModifiedTime").longValue()));
        }
        if (root.path("dataSetId").isIntegralNumber()) {
            fileMetaBuilder.setDataSetId(Int64Value.of(root.get("dataSetId").longValue()));
        }

        if (root.path("securityCategories").isArray()) {
            for (JsonNode node : root.path("securityCategories")) {
                if (node.isIntegralNumber()) {
                    fileMetaBuilder.addSecurityCategories(node.longValue());
                }
            }
        }

        if (root.path("assetIds").isArray()) {
            for (JsonNode node : root.path("assetIds")) {
                if (node.isIntegralNumber()) {
                    fileMetaBuilder.addAssetIds(node.longValue());
                }
            }
        }

        if (root.path("metadata").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fieldIterator = root
                    .path("metadata").fields();
            while (fieldIterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldIterator.next();
                if (entry.getValue().isTextual()) {
                    fileMetaBuilder
                            .putMetadata(entry.getKey(), entry.getValue().textValue());
                }
            }
        }

        if (root.path("labels").isArray()) {
            for (JsonNode node : root.path("labels")) {
                if (node.path("externalId").isTextual()) {
                    fileMetaBuilder.addLabels(node.path("externalId").textValue());
                }
            }
        }

        return fileMetaBuilder.build();
    }

    /**
     * Builds a request insert item object from <code>FileMetadata</code>.
     *
     * An insert item object creates a new file (metadata) data object in the Cognite system.
     *
     * @param element
     * @return
     * @throws Exception
     */
    public static Map<String, Object> toRequestInsertItem(FileMetadata element) throws Exception {
        // Note that "id" cannot be a part of an insert request.
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        // Required fields
        if (element.hasName()) {
            mapBuilder.put("name", element.getName().getValue());
        } else {
            throw new Exception("Unable to find attribute [name] in the file metadata/header object. "
                    + "externalId: [" + element.getExternalId().getValue() + "].");
        }

        // Optional fields
        if (element.hasDirectory()) {
            mapBuilder.put("directory", element.getDirectory().getValue());
        }

        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId().getValue());
        }

        if (element.hasSource()) {
            mapBuilder.put("source", element.getSource().getValue());
        }
        if (element.hasMimeType()) {
            mapBuilder.put("mimeType", element.getMimeType().getValue());
        }
        if (element.hasSourceCreatedTime()) {
            mapBuilder.put("sourceCreatedTime", element.getSourceCreatedTime().getValue());
        }
        if (element.hasSourceModifiedTime()) {
            mapBuilder.put("sourceModifiedTime", element.getSourceModifiedTime().getValue());
        }
        if (element.getAssetIdsCount() > 0) {
            mapBuilder.put("assetIds", element.getAssetIdsList());
        }
        if (element.getMetadataCount() > 0) {
            mapBuilder.put("metadata", element.getMetadataMap());
        }
        if (element.hasDataSetId()) {
            mapBuilder.put("dataSetId", element.getDataSetId().getValue());
        }
        if (element.getLabelsCount() > 0) {
            List<Map<String, String>> labels = new ArrayList<>();
            for (String label : element.getLabelsList()) {
                labels.add(ImmutableMap.of("externalId", label));
            }
            mapBuilder.put("labels", labels);
        }

        return mapBuilder.build();
    }

    /**
     * Builds a request update item object from <code>FileMetadata</code>.
     *
     * An update item object updates an existing file (metadata) object with new values for all provided fields.
     * Fields that are not in the update object retain their original value.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestUpdateItem(FileMetadata element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();


        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId().getValue());
        } else {
            mapBuilder.put("id", element.getId().getValue());
        }

        // the parameter "name" cannot be updated.
        if (element.hasDirectory()) {
            updateNodeBuilder.put("directory", ImmutableMap.of("set", element.getDirectory().getValue()));
        }
        if (element.hasMimeType()) {
            updateNodeBuilder.put("mimeType", ImmutableMap.of("set", element.getMimeType().getValue()));
        }
        if (element.hasSource()) {
            updateNodeBuilder.put("source", ImmutableMap.of("set", element.getSource().getValue()));
        }
        if (element.hasSourceCreatedTime()) {
            updateNodeBuilder.put("sourceCreatedTime", ImmutableMap.of("set", element.getSourceCreatedTime().getValue()));
        }
        if (element.hasSourceModifiedTime()) {
            updateNodeBuilder.put("sourceModifiedTime", ImmutableMap.of("set", element.getSourceModifiedTime().getValue()));
        }
        if (element.getAssetIdsCount() > 0) {
            updateNodeBuilder.put("assetIds", ImmutableMap.of("set", element.getAssetIdsList()));
        }
        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("add", element.getMetadataMap()));
        }
        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId().getValue()));
        }
        if (element.getLabelsCount() > 0) {
            List<Map<String, String>> labels = new ArrayList<>();
            for (String label : element.getLabelsList()) {
                labels.add(ImmutableMap.of("externalId", label));
            }
            updateNodeBuilder.put("labels", ImmutableMap.of("add", labels));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    /**
     * Builds a request insert item object from <code>FileMetadata</code>.
     *
     * A replace item object replaces an existing file (metadata) object with new values for all provided fields.
     * Fields that are not in the update object are set to null.
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestReplaceItem(FileMetadata element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();
        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId().getValue());
        } else {
            mapBuilder.put("id", element.getId().getValue());
        }

        // the parameter "name" cannot be updated.
        if (element.hasDirectory()) {
            updateNodeBuilder.put("directory", ImmutableMap.of("set", element.getDirectory().getValue()));
        } else {
            updateNodeBuilder.put("directory", ImmutableMap.of("setNull", true));
        }

        if (element.hasMimeType()) {
            updateNodeBuilder.put("mimeType", ImmutableMap.of("set", element.getMimeType().getValue()));
        } else {
            updateNodeBuilder.put("mimeType", ImmutableMap.of("setNull", true));
        }

        if (element.hasSource()) {
            updateNodeBuilder.put("source", ImmutableMap.of("set", element.getSource().getValue()));
        } else {
            updateNodeBuilder.put("source", ImmutableMap.of("setNull", true));
        }

        if (element.hasSourceCreatedTime()) {
            updateNodeBuilder.put("sourceCreatedTime", ImmutableMap.of("set", element.getSourceCreatedTime().getValue()));
        } else {
            updateNodeBuilder.put("sourceCreatedTime", ImmutableMap.of("setNull", true));
        }

        if (element.hasSourceModifiedTime()) {
            updateNodeBuilder.put("sourceModifiedTime", ImmutableMap.of("set", element.getSourceModifiedTime().getValue()));
        } else {
            updateNodeBuilder.put("sourceModifiedTime", ImmutableMap.of("setNull", true));
        }

        if (element.getAssetIdsCount() > 0) {
            updateNodeBuilder.put("assetIds", ImmutableMap.of("set", element.getAssetIdsList()));
        } else {
            updateNodeBuilder.put("assetIds", ImmutableMap.of("set", ImmutableList.<Long>of()));
        }

        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", element.getMetadataMap()));
        } else {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", ImmutableMap.<String, String>of()));
        }

        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId().getValue()));
        } else {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("setNull", true));
        }

        if (element.getLabelsCount() > 0) {
            List<Map<String, String>> labels = new ArrayList<>();
            for (String label : element.getLabelsList()) {
                labels.add(ImmutableMap.of("externalId", label));
            }
            // TODO change to "set" when the api has been updated to support the operation
            updateNodeBuilder.put("labels", ImmutableMap.of("add", labels));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    /**
     * Builds a request add assetId item object from <code>FileMetadata</code>.
     *
     * This method creates a special purpose item for adding assetIds to an existing file item. It is used for
     * posting files with very a very high number of assetIds. The Cognite API has a limit of 1k assetIds per request
     * so large assetId arrays need to be split into multiple update requests.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestAddAssetIdsItem(FileMetadata element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();


        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId().getValue());
        } else {
            mapBuilder.put("id", element.getId().getValue());
        }

        if (element.getAssetIdsCount() > 0) {
            updateNodeBuilder.put("assetIds", ImmutableMap.of("add", element.getAssetIdsList()));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    private static String buildErrorMessage(String fieldName, String inputElement) {
        return logPrefix + "Unable to parse attribute: "+ fieldName + ". Item exerpt: "
                + inputElement.substring(0, Math.min(inputElement.length() - 1, MAX_LOG_ELEMENT_LENGTH));
    }
}
